// Package p contains a Google Cloud Function that fetches data from a Google Sheet
// and dispatches it in chunks to a Google Workflow. It dynamically adjusts its
// processing load based on a 'SCOPE' variable from Secret Manager.
package p

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
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
)

// main is the entry point for the application. It sets up the HTTP server.
// This is required for 2nd Gen Cloud Functions / Cloud Run.
func main() {
	// The init() function will be called automatically before main().

	// Register the HTTP handler function.
	http.HandleFunc("/", DispatchSheetDataToWorkflows)

	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	// Start HTTP server.
	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

// init runs once per function instance, initializing clients and configuration.
func init() {
	ctx := context.Background()
	var err error

	// 1. Get Project ID from environment variables, as it's needed for multiple clients.
	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		log.Fatalf("Missing required environment variable: GCP_PROJECT_ID")
	}

	// 2. Initialize Secret Manager Client
	secretManagerClient, err = secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create Secret Manager client: %v", err)
	}

	// 3. Fetch the SCOPE from Secret Manager
	scope, err = getScopeFromSecretManager(ctx, projectID)
	if err != nil {
		// In a serverless environment, failing to get the scope might be critical.
		// However, we can also default to a safe mode. Here, we'll default to "Test".
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
		log.Fatalf("Unable to create Google Default Client for Sheets: %v", err)
	}
	sheetsService, err = sheets.NewService(ctx, option.WithHTTPClient(sheetsHTTPClient))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	// 6. Initialize Google Workflows Executions Client
	workflowsClient, err = workflows.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create Workflows Executions client: %v", err)
	}

	// 7. Get and validate environment variables for the workflow
	location := os.Getenv("GCP_LOCATION")
	workflowName := os.Getenv("WORKFLOW_NAME")
	if location == "" || workflowName == "" {
		log.Fatalf("Missing required environment variables: GCP_LOCATION, WORKFLOW_NAME")
	}
	workflowParent = fmt.Sprintf("projects/%s/locations/%s/workflows/%s", projectID, location, workflowName)
}

// getScopeFromSecretManager fetches the latest version of the 'SCOPE' secret.
func getScopeFromSecretManager(ctx context.Context, projectID string) (string, error) {
	secretName := os.Getenv("SCOPE_SECRET_NAME")
	if secretName == "" {
		return "", fmt.Errorf("environment variable SCOPE_SECRET_NAME is not set")
	}

	// Build the resource name of the secret version.
	// Using "latest" is convenient for automatically picking up new versions.
	versionName := fmt.Sprintf("projects/%s/secrets/%s/versions/latest", projectID, secretName)

	// Create the request to access the secret version.
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: versionName,
	}

	// Call the API.
	result, err := secretManagerClient.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %w", err)
	}

	// The secret payload is returned as a byte slice.
	// We convert it to a string, assuming it's simple text ('Test' or 'Prod').
	return string(result.Payload.Data), nil
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

	// Fetch all data from the sheet
	resp, err := sheetsService.Spreadsheets.Values.Get(spreadsheetID, sheetName).Do()
	if err != nil {
		log.Printf("Unable to retrieve data from sheet: %v", err)
		http.Error(w, "Failed to retrieve data from Google Sheet.", http.StatusInternalServerError)
		return
	}

	if len(resp.Values) <= 1 { // No data or only a header row
		fmt.Fprintln(w, "No data rows to process.")
		return
	}

	headerRow := resp.Values[0]
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

		// Ensure we don't go out of bounds, especially for the last chunk or in test mode.
		if end > totalRows {
			end = totalRows
		}

		// If start is past the end, we've processed all we need to.
		if start >= end {
			break
		}

		chunk := dataRows[start:end]

		// The payload for the workflow must mimic the structure of the original http.get call.
		// The workflow expects a `body` field containing the sheet data.
		workflowPayload := map[string]interface{}{
			"body": map[string]interface{}{
				"valueRanges": []map[string]interface{}{
					{
						"values": append([][]interface{}{headerRow}, chunk...),
					},
				},
			},
		}

		// Marshal the payload into a JSON string for the workflow argument.
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
				Parent: workflowParent,
				Execution: &executionspb.Execution{
					Argument: arguments,
				},
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
	if len(executionErrors) > 0 {
		var errorMessages []string
		for err := range executionErrors {
			errorMessages = append(errorMessages, err.Error())
		}
		log.Printf("Completed with %d errors: %v", len(errorMessages), errorMessages)
		http.Error(w, fmt.Sprintf("Completed with %d errors.", len(errorMessages)), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Successfully dispatched %d chunks to workflow '%s' in '%s' mode.", numChunks, os.Getenv("WORKFLOW_NAME"), scope)
}
