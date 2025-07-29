// An executable application must be in package main.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	workflows "cloud.google.com/go/workflows/executions/apiv1"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// Global clients and configuration.
var (
	sheetsService       *sheets.Service
	workflowsClient     *workflows.Client
	secretManagerClient *secretmanager.Client
	workflowParent      string
	scope               string // Holds the execution scope ('Test' or 'Prod')
	chunkSize           int    // Number of rows per workflow execution
	maxTestBatches      = 2    // Maximum number of batches to process in 'Test' mode

	// A sync.Once will ensure our initialization logic runs exactly once.
	initOnce sync.Once
	initErr  error // To store any error that occurs during initialization.
)

// main is the entry point for the application. It sets up the HTTP server.
func main() {
	// Register the HTTP handler function. We wrap it to ensure initialization runs first.
	http.HandleFunc("/", safeHandler(DispatchSheetDataToWorkflows))

	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	// Start HTTP server.
	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// safeHandler wraps an http.HandlerFunc to ensure initialization is performed
// safely before the actual handler is called.
func safeHandler(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		initOnce.Do(initialize)
		if initErr != nil {
			log.Printf("Initialization failed: %v", initErr)
			http.Error(w, "Internal Server Error: could not initialize service", http.StatusInternalServerError)
			return
		}
		h(w, r)
	}
}

// initialize contains the logic that was previously in the init() function.
func initialize() {
	ctx := context.Background()
	var err error

	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		initErr = fmt.Errorf("missing required environment variable: GCP_PROJECT_ID")
		return
	}

	secretManagerClient, err = secretmanager.NewClient(ctx)
	if err != nil {
		initErr = fmt.Errorf("failed to create Secret Manager client: %w", err)
		return
	}

	scope, err = getScopeFromSecretManager(ctx, projectID)
	if err != nil {
		log.Printf("Warning: Failed to retrieve SCOPE from Secret Manager: %v. Defaulting to 'Test' mode.", err)
		scope = "Test"
	}
	log.Printf("Running in SCOPE: %s", scope)

	if scope == "Test" {
		chunkSize = 5
	} else {
		chunkSize = 250
	}
	log.Printf("Using chunk size: %d", chunkSize)

	sheetsHTTPClient, err := google.DefaultClient(ctx, sheets.SpreadsheetsReadonlyScope)
	if err != nil {
		initErr = fmt.Errorf("unable to create Google Default Client for Sheets: %w", err)
		return
	}
	sheetsService, err = sheets.NewService(ctx, option.WithHTTPClient(sheetsHTTPClient))
	if err != nil {
		initErr = fmt.Errorf("unable to retrieve Sheets client: %w", err)
		return
	}

	workflowsClient, err = workflows.NewClient(ctx)
	if err != nil {
		initErr = fmt.Errorf("failed to create Workflows Executions client: %w", err)
		return
	}

	location := os.Getenv("GCP_LOCATION")
	workflowName := os.Getenv("WORKFLOW_NAME")
	if location == "" || workflowName == "" {
		initErr = fmt.Errorf("missing required environment variables: GCP_LOCATION, WORKFLOW_NAME")
		return
	}
	workflowParent = fmt.Sprintf("projects/%s/locations/%s/workflows/%s", projectID, location, workflowName)
	log.Println("Initialization successful.")
}

// getScopeFromSecretManager fetches the 'SCOPE' secret.
func getScopeFromSecretManager(ctx context.Context, projectID string) (string, error) {
	secretName := os.Getenv("SCOPE_SECRET_NAME")
	if secretName == "" {
		return "", fmt.Errorf("environment variable SCOPE_SECRET_NAME is not set")
	}
	versionName := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", projectID, secretName)
	req := &secretmanagerpb.AccessSecretVersionRequest{Name: versionName}

	result, err := secretManagerClient.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %w", err)
	}
	return string(result.Payload.Data), nil
}

// validateHeader robustly checks the header row and returns an error if validation fails.
func validateHeader(headerRow []interface{}) error {
	mandatoryColumns := []string{"ID", "Company_Name"}
	headerSet := make(map[string]bool)

	for _, col := range headerRow {
		if col != nil {
			colStr := strings.TrimSpace(fmt.Sprint(col))
			if colStr != "" {
				headerSet[colStr] = true
			}
		}
	}

	detectedColumns := make([]string, 0, len(headerSet))
	for colName := range headerSet {
		detectedColumns = append(detectedColumns, colName)
	}
	log.Printf("🔎 Detected header columns: [%s]", strings.Join(detectedColumns, ", "))

	var missingMandatory []string
	for _, mCol := range mandatoryColumns {
		if !headerSet[mCol] {
			missingMandatory = append(missingMandatory, mCol)
		}
	}

	if len(missingMandatory) > 0 {
		return fmt.Errorf("the header row is missing mandatory columns: [%s]", strings.Join(missingMandatory, ", "))
	}
	return nil // Success
}

// DispatchSheetDataToWorkflows reads a Google Sheet and triggers workflows.
func DispatchSheetDataToWorkflows(w http.ResponseWriter, r *http.Request) {
	spreadsheetID := os.Getenv("SPREADSHEET_ID")
	sheetName := os.Getenv("SHEET_NAME")
	if spreadsheetID == "" || sheetName == "" {
		http.Error(w, "Server configuration error: SPREADSHEET_ID and SHEET_NAME must be set.", http.StatusInternalServerError)
		return
	}

	readRange := fmt.Sprintf("%s!A:ZZ", sheetName)
	log.Printf("Attempting to read data from sheet '%s' with range '%s'", spreadsheetID, readRange)

	resp, err := sheetsService.Spreadsheets.Values.Get(spreadsheetID, readRange).Do()
	if err != nil {
		log.Printf("Error: Unable to retrieve data from sheet. Error: %v", err)
		http.Error(w, "Failed to retrieve data from Google Sheet.", http.StatusInternalServerError)
		return
	}
	log.Printf("✅ Successfully read %d rows from sheet.", len(resp.Values))

	if len(resp.Values) == 0 {
		log.Println("Sheet is empty. No data to process.")
		fmt.Fprintln(w, "Sheet is empty, no action taken.")
		return
	}

	var headerRow []interface{}
	var dataRows [][]interface{}
	headerIndex := -1

	// Find the first non-empty row to ensure it's the header.
	for i, row := range resp.Values {
		// A row is considered non-empty if it has at least one cell with content.
		if len(row) > 0 {
			headerRow = row
			headerIndex = i
			break
		}
	}

	if headerIndex == -1 {
		log.Println("Sheet contains only empty rows. No data to process.")
		fmt.Fprintln(w, "Sheet contains only empty rows, no action taken.")
		return
	}

	log.Printf("Header row identified at sheet row %d.", headerIndex+1)
	dataRows = resp.Values[headerIndex+1:]

	// Validate the header and FAIL FAST if it's invalid.
	if err := validateHeader(headerRow); err != nil {
		log.Printf("❌ Header validation failed: %v", err)
		http.Error(w, "Header validation failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	log.Println("✅ Header validation successful.")
	if len(dataRows) == 0 {
		log.Println("No data rows found after the header. No action taken.")
		fmt.Fprintln(w, "No data rows found after the header.")
		return
	}
	log.Printf("Found %d data rows to process.", len(dataRows))

	var wg sync.WaitGroup
	totalRows := len(dataRows)
	numChunks := (totalRows + chunkSize - 1) / chunkSize

	if scope == "Test" && numChunks > maxTestBatches {
		numChunks = maxTestBatches
		log.Printf("In 'Test' scope, processing a maximum of %d batches.", maxTestBatches)
	}

	executionErrors := make(chan error, numChunks)

	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > totalRows {
			end = totalRows
		}
		if start >= end {
			break
		}
		chunk := dataRows[start:end]

		payloadRows := make([][]interface{}, 0, len(chunk)+1)
		payloadRows = append(payloadRows, headerRow)
		payloadRows = append(payloadRows, chunk...)

		workflowPayload := map[string]interface{}{"body": map[string]interface{}{"valueRanges": []interface{}{map[string]interface{}{"values": payloadRows}}}}
		args, err := json.Marshal(workflowPayload)
		if err != nil {
			log.Printf("Failed to marshal chunk %d: %v", i, err)
			executionErrors <- err
			continue
		}

		wg.Add(1)
		go func(chunkNum int, arguments string) {
			defer wg.Done()
			req := &executionspb.CreateExecutionRequest{Parent: workflowParent, Execution: &executionspb.Execution{Argument: arguments}}
			_, err := workflowsClient.CreateExecution(r.Context(), req)
			if err != nil {
				log.Printf("Failed to execute workflow for chunk %d: %v", chunkNum, err)
				executionErrors <- err
			} else {
				log.Printf("Successfully triggered workflow for chunk %d.", chunkNum)
			}
		}(i, string(args))
	}

	wg.Wait()
	close(executionErrors)

	var errorMessages []string
	for err := range executionErrors {
		errorMessages = append(errorMessages, err.Error())
	}

	if len(errorMessages) > 0 {
		log.Printf("Completed with %d errors: %v", len(errorMessages), errorMessages)
		http.Error(w, fmt.Sprintf("Completed with %d errors.", len(errorMessages)), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Successfully dispatched %d chunks to workflow '%s' in '%s' mode.", numChunks, os.Getenv("WORKFLOW_NAME"), scope)
}
