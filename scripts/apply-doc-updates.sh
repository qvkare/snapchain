#!/bin/bash
set -euo pipefail

readonly UPDATES_JSON="$1"

validate_input() {
    if [[ -z "$UPDATES_JSON" ]]; then
        echo "Error: No updates JSON provided" >&2
        exit 1
    fi
    
    if ! command -v jq &> /dev/null; then
        echo "Error: jq is required but not installed" >&2
        exit 1
    fi
    
    # Validate JSON format
    if ! echo "$UPDATES_JSON" | jq empty 2>/dev/null; then
        echo "Error: Invalid JSON format in updates" >&2
        exit 1
    fi
}

backup_existing_files() {
    local updates="$1"
    
    echo "Creating backups of existing files..."
    
    echo "$updates" | jq -r '.updates // {} | keys[]' 2>/dev/null | while read -r file_path; do
        if [[ -f "$file_path" ]]; then
            local backup_path="${file_path}.backup-$(date +%s)"
            cp "$file_path" "$backup_path"
            echo "Backed up $file_path to $backup_path"
        fi
    done
}

ensure_directory_exists() {
    local file_path="$1"
    local dir_path
    dir_path=$(dirname "$file_path")
    
    if [[ ! -d "$dir_path" ]]; then
        echo "Creating directory: $dir_path"
        mkdir -p "$dir_path"
    fi
}

apply_file_updates() {
    local updates="$1"
    local files_updated=0
    
    echo "Applying documentation updates..."
    
    # Process each file update
    echo "$updates" | jq -r '.updates // {} | to_entries[] | @base64' | while read -r entry; do
        local decoded
        decoded=$(echo "$entry" | base64 -d)
        
        local file_path
        local content
        file_path=$(echo "$decoded" | jq -r '.key')
        content=$(echo "$decoded" | jq -r '.value')
        
        if [[ -z "$file_path" || "$file_path" == "null" ]]; then
            echo "Warning: Empty file path in updates, skipping..."
            continue
        fi
        
        if [[ -z "$content" || "$content" == "null" ]]; then
            echo "Warning: Empty content for $file_path, skipping..."
            continue
        fi
        
        # Ensure the directory exists
        ensure_directory_exists "$file_path"
        
        # Write the new content
        echo "Updating: $file_path"
        echo "$content" > "$file_path"
        
        # Verify the file was written correctly
        if [[ -f "$file_path" ]]; then
            local line_count
            line_count=$(wc -l < "$file_path")
            echo "  ✓ Written successfully ($line_count lines)"
            files_updated=$((files_updated + 1))
        else
            echo "  ✗ Failed to write file"
        fi
    done
    
    echo "Documentation update complete. Files updated: $files_updated"
}

validate_updates() {
    local updates="$1"
    
    echo "Validating documentation updates..."
    
    # Check if any files need updates
    local needs_update
    needs_update=$(echo "$updates" | jq -r '.needs_update // false')
    
    if [[ "$needs_update" != "true" ]]; then
        echo "No updates needed according to analysis."
        return 1
    fi
    
    # Check if updates object exists and has content
    local update_count
    update_count=$(echo "$updates" | jq -r '.updates // {} | length')
    
    if [[ "$update_count" == "0" ]]; then
        echo "No file updates provided in the analysis."
        return 1
    fi
    
    echo "Validation passed. $update_count files to update."
    return 0
}

log_changes_summary() {
    local updates="$1"
    
    local summary
    summary=$(echo "$updates" | jq -r '.changes_summary // "No summary provided"')
    
    local affected_files
    affected_files=$(echo "$updates" | jq -r '.files_affected[]?' 2>/dev/null | tr '\n' ' ' || echo "None listed")
    
    echo ""
    echo "CHANGES SUMMARY:"
    echo "Description: $summary"
    echo "Files affected: $affected_files"
    echo ""
}

check_snapchain_docs_structure() {
    # Ensure we're in the right directory structure
    if [[ ! -d "site/docs/pages/reference" ]]; then
        echo "Error: Not in Snapchain project root or documentation structure not found" >&2
        exit 1
    fi
    
    echo "Snapchain documentation structure verified."
}

main() {
    echo "Starting documentation updates application..."
    
    validate_input
    check_snapchain_docs_structure
    
    if ! validate_updates "$UPDATES_JSON"; then
        echo "No documentation updates to apply."
        exit 0
    fi
    
    log_changes_summary "$UPDATES_JSON"
    backup_existing_files "$UPDATES_JSON"
    apply_file_updates "$UPDATES_JSON"
    
    echo "Documentation updates completed successfully."
}

main "$@" 