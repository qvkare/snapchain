#!/bin/bash
set -euo pipefail

readonly DIFF_CONTENT="$1"
readonly GEMINI_API_URL="https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent"

check_requirements() {
    if [[ -z "${GEMINI_API_KEY:-}" ]]; then
        echo "Error: GEMINI_API_KEY not set" >&2
        exit 1
    fi
    
    if [[ -z "$DIFF_CONTENT" ]]; then
        echo "No changes detected"
        if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
            echo "needs_update=false" >> "$GITHUB_OUTPUT"
        fi
        exit 0
    fi
    
    if ! command -v jq &> /dev/null; then
        echo "Error: jq is required but not installed" >&2
        exit 1
    fi
}

build_snapchain_context() {
    local context=""
    
    # Add Snapchain project context
    context+="SNAPCHAIN PROJECT CONTEXT:\n"
    context+="- Farcaster protocol implementation in Rust\n"
    context+="- Decentralized P2P network for social protocol data\n"
    context+="- gRPC and HTTP APIs for data access\n"
    context+="- Protocol Buffers for data serialization\n"
    context+="- Byzantine Fault Tolerant consensus\n\n"
    
    # Get current documentation structure
    if [[ -d "site/docs/pages/reference" ]]; then
        context+="CURRENT DOCUMENTATION STRUCTURE:\n"
        find site/docs/pages/reference -name "*.md" | head -20 | while read -r file; do
            context+="- $file\n"
        done
        context+="\n"
    fi
    
    # Get proto files context for API understanding
    if [[ -d "src/proto" ]]; then
        context+="PROTO FILES CONTEXT:\n"
        for proto in src/proto/*.proto; do
            [[ -f "$proto" ]] || continue
            context+="=== $(basename "$proto") ===\n"
            head -40 "$proto" | while read -r line; do
                context+="$line\n"
            done
            context+="\n"
        done
    fi
    
    # Add sample documentation format
    if [[ -f "site/docs/pages/reference/grpcapi/casts.md" ]]; then
        context+="DOCUMENTATION FORMAT EXAMPLE:\n"
        context+="=== casts.md sample ===\n"
        head -25 "site/docs/pages/reference/grpcapi/casts.md" | while read -r line; do
            context+="$line\n"
        done
        context+="\n"
    fi
    
    echo -e "$context"
}

create_analysis_prompt() {
    local context="$1"
    local diff="$2"
    
    cat << EOF
You are analyzing Snapchain codebase changes for documentation updates. Snapchain is a Rust implementation of the Farcaster protocol.

$context

ANALYZE THIS DIFF FOR DOCUMENTATION IMPACTS:
$diff

ANALYSIS REQUIREMENTS:
1. Focus on changes to:
   - src/proto/*.proto files -> affects grpcapi/*.md and datatypes/*.md
   - src/network/server.rs -> affects grpcapi/*.md 
   - src/network/http_server.rs -> affects httpapi/*.md
   - src/core/types.rs -> affects datatypes/*.md

2. Documentation mapping:
   - Proto service methods -> site/docs/pages/reference/grpcapi/[service].md
   - Proto message types -> site/docs/pages/reference/datatypes/[type].md
   - HTTP endpoints -> site/docs/pages/reference/httpapi/[service].md

3. Look for:
   - New RPC methods or HTTP endpoints
   - Changed method signatures or parameters
   - New message fields or types
   - Deprecated methods
   - Parameter name changes (like pageToken vs page_token inconsistencies)

4. Generate complete markdown content following the existing format with proper tables and descriptions.

Return ONLY valid JSON in this exact format:
{
  "needs_update": boolean,
  "files_affected": ["path/to/file.md"],
  "changes_summary": "brief description of what changed",
  "updates": {
    "site/docs/pages/reference/path/file.md": "complete new file content here"
  }
}
EOF
}

call_gemini_api() {
    local prompt="$1"
    
    local payload
    payload=$(jq -n \
        --arg text "$prompt" \
        '{
            contents: [{
                parts: [{
                    text: $text
                }]
            }],
            generationConfig: {
                temperature: 0.1,
                maxOutputTokens: 8192,
                responseMimeType: "application/json"
            }
        }')
    
    local response
    response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -H "x-goog-api-key: $GEMINI_API_KEY" \
        "$GEMINI_API_URL" \
        -d "$payload")
    
    if [[ -z "$response" ]]; then
        echo "Error: Empty response from Gemini API" >&2
        exit 1
    fi
    
    echo "$response"
}

parse_and_output_results() {
    local response="$1"
    
    # Extract the JSON content from Gemini response
    local result
    result=$(echo "$response" | jq -r '.candidates[0].content.parts[0].text' 2>/dev/null || echo '{}')
    
    if [[ "$result" == "{}" || "$result" == "null" ]]; then
        echo "Error: Could not parse Gemini response" >&2
        echo "Response was: $response" >&2
        if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
            echo "needs_update=false" >> "$GITHUB_OUTPUT"
        fi
        exit 1
    fi
    
    # Validate JSON structure
    local needs_update
    needs_update=$(echo "$result" | jq -r '.needs_update // false' 2>/dev/null || echo "false")
    
    # Output for GitHub Actions (if running in Actions)
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        echo "needs_update=$needs_update" >> "$GITHUB_OUTPUT"
        echo "updates<<EOF" >> "$GITHUB_OUTPUT"
        echo "$result" >> "$GITHUB_OUTPUT"
        echo "EOF" >> "$GITHUB_OUTPUT"
    fi
    
    # Log summary
    local summary
    summary=$(echo "$result" | jq -r '.changes_summary // "No summary available"')
    echo "Analysis complete."
    echo "Updates needed: $needs_update"
    echo "Summary: $summary"
    
    if [[ "$needs_update" == "true" ]]; then
        echo "Files to be updated:"
        echo "$result" | jq -r '.files_affected[]?' 2>/dev/null || echo "  No files listed"
    fi
}

main() {
    echo "Starting Snapchain documentation analysis with Gemini 2.5 Flash..."
    
    check_requirements
    
    local context
    context=$(build_snapchain_context)
    
    local prompt
    prompt=$(create_analysis_prompt "$context" "$DIFF_CONTENT")
    
    echo "Calling Gemini API..."
    local response
    response=$(call_gemini_api "$prompt")
    
    echo "Processing results..."
    parse_and_output_results "$response"
    
    echo "Documentation analysis completed successfully."
}

main "$@" 