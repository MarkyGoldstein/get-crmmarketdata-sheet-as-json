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
		// initOnce.Do will call initialize() only on the very first request.
		// Subsequent calls will do nothing.
		initOnce.Do(initialize)

		// If initialization failed, return an internal server error.
		if initErr != nil {
			log.Printf("Initialization failed: %v", initErr)
			http.Error(w, "Internal Server Error: could not initialize service", http.StatusInternalServerError)
			return
		}

		// If initialization was successful, call the actual handler.
		h(w, r)
	}
}

// initialize contains the logic that was previously in the init() function.
// It sets up all clients and configurations.
func initialize() {
	ctx := context.Background()
	var err error

	// 1. Get Project ID from environment variables.
	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		initErr = fmt.Errorf("missing required environment variable: GCP_PROJECT_ID")
		return
	}

	// 2. Initialize Secret Manager Client
	secretManagerClient, err = secretmanager.NewClient(ctx)
	if err != nil {
		initErr = fmt.Errorf("failed to create Secret Manager client: %w", err)
		return
	}

	// 3. Fetch the SCOPE from Secret Manager
	scope, err = getScopeFromSecretManager(ctx, projectID)
	if err != nil {
		log.Printf("Warning: Failed to retrieve SCOPE from Secret Manager: %v. Defaulting to 'Test' mode.", err)
		scope = "Test"
	}
	log.Printf("Running in SCOPE: %s", scope)

	// 4. Configure chunk size based on the scope
	if scope == "Test" {
		chunkSize = 5
	} else {
		chunkSize = 250
	}
	log.Printf("Using chunk size: %d", chunkSize)

	// 5. Initialize Google Sheets Client
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

	// 6. Initialize Google Workflows Executions Client
	workflowsClient, err = workflows.NewClient(ctx)
	if err != nil {
		initErr = fmt.Errorf("failed to create Workflows Executions client: %w", err)
		return
	}

	// 7. Get and validate environment variables for the workflow
	location := os.Getenv("GCP_LOCATION")
	workflowName := os.Getenv("WORKFLOW_NAME")
	if location == "" || workflowName == "" {
		initErr = fmt.Errorf("missing required environment variables: GCP_LOCATION, WORKFLOW_NAME")
		return
	}
	workflowParent = fmt.Sprintf("projects/%s/locations/%s/workflows/%s", projectID, location, workflowName)
	log.Println("Initialization successful.")
}

// getScopeFromSecretManager fetches the latest version of the 'SCOPE' secret.
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

// validateHeader checks if the required columns are present in the header row.
func validateHeader(headerRow []interface{}) {
	// Define which columns are mandatory.
	mandatoryColumns := []string{"ID", "Company_Name"}

	// Create a set for efficient lookup of existing header columns.
	headerSet := make(map[string]bool)

	// This loop safely converts any cell type (number, text, etc.)
	// to a string and trims whitespace for a reliable comparison.
	for _, col := range headerRow {
		if col != nil {
			colStr := strings.TrimSpace(fmt.Sprint(col))
			if colStr != "" { // Avoid adding empty headers
				headerSet[colStr] = true
			}
		}
	}

	// NEW: Log all detected column names for debugging purposes.
	detectedColumns := make([]string, 0, len(headerSet))
	for colName := range headerSet {
		detectedColumns = append(detectedColumns, colName)
	}
	log.Printf("🔎 Detected header columns: [%s]", strings.Join(detectedColumns, ", "))

	// Check for missing mandatory columns.
	var missingMandatory []string
	for _, mCol := range mandatoryColumns {
		if !headerSet[mCol] {
			missingMandatory = append(missingMandatory, mCol)
		}
	}

	// If any mandatory columns are missing, log a warning.
	if len(missingMandatory) > 0 {
		log.Printf("⚠️ Warning: The following mandatory header columns are missing from the sheet: %s", strings.Join(missingMandatory, ", "))
	}
}

// DispatchSheetDataToWorkflows is an HTTP Cloud Function that reads a Google Sheet
// and triggers a Google Workflow for each chunk of data.
func DispatchSheetDataToWorkflows(w http.ResponseWriter, r *http.Request) {
	// Get sheet configuration from environment variables
	spreadsheetID := os.Getenv("SPREADSHEET_ID")
	sheetName := os.Getenv("SHEET_NAME")
	if spreadsheetID == "" || sheetName == "" {
		http.Error(w, "Server configuration error: SPREADSHEET_ID and SHEET_NAME must be set.", http.StatusInternalServerError)
		return
	}

	log.Printf("Attempting to open Google Sheet ID: %s, Name: %s", spreadsheetID, sheetName)
	// Fetch all data from the sheet
	resp, err := sheetsService.Spreadsheets.Values.Get(spreadsheetID, sheetName).Do()
	if err != nil {
		// Log a detailed warning if the sheet could not be opened/read.
		log.Printf("Error: Unable to retrieve data from sheet ID '%s', Name '%s'. Error: %v", spreadsheetID, sheetName, err)
		http.Error(w, "Failed to retrieve data from Google Sheet.", http.StatusInternalServerError)
		return
	}
	// Log a confirmation message upon successful access.
	log.Printf("✅ Successfully opened and read data from Google Sheet ID: %s, Name: %s. Found %d total rows.", spreadsheetID, sheetName, len(resp.Values))

	if len(resp.Values) <= 1 { // No data or only a header row
		log.Println("No data rows to process.")
		fmt.Fprintln(w, "No data rows to process.")
		return
	}

	headerRow := resp.Values[0]
	validateHeader(headerRow)

	dataRows := resp.Values[1:]
	var wg sync.WaitGroup

	// Determine the number of chunks to process
	totalRows := len(dataRows)
	numChunks := (totalRows + chunkSize - 1) / chunkSize

	// In 'Test' mode, limit the number of batches
	if scope == "Test" && numChunks > maxTestBatches {
		numChunks = maxTestBatches
		log.Printf("In 'Test' scope, processing a maximum of %d batches.", maxTestBatches)
	}

	executionErrors := make(chan error, numChunks)

	// Process rows in chunks
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

		// Create a new slice for the payload that includes the header row.
		payloadRows := make([][]interface{}, 0, len(chunk)+1)
		payloadRows = append(payloadRows, headerRow)
		payloadRows = append(payloadRows, chunk...)

		workflowPayload := map[string]interface{}{
			"body": map[string]interface{}{
				"valueRanges": []interface{}{
					map[string]interface{}{
						"values": payloadRows, // Use the slice that includes the header
					},
				},
			},
		}

		args, err := json.Marshal(workflowPayload)
		if err != nil {
			log.Printf("Failed to marshal chunk %d: %v", i, err)
			executionErrors <- err
			continue
		}

		wg.Add(1)
		go func(chunkNum int, arguments string) {
			defer wg.Done()
			req := &executionspb.CreateExecutionRequest{
				Parent:    workflowParent,
				Execution: &executionspb.Execution{Argument: arguments},
			}
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

	// Report the outcome
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
