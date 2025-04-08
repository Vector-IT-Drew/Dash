from flask import Blueprint, request, Response, render_template_string
import os
import logging
import json
import re
from datetime import datetime

log_viewer_bp = Blueprint('log_viewer', __name__)
logger = logging.getLogger("api")

@log_viewer_bp.route('/admin/view-logs', methods=['GET'])
def view_logs():
    # Get API key from query parameter or header
    api_key = request.args.get('api_key') or request.headers.get('X-API-Key')
    
    # Check against environment variable
    admin_key = os.getenv("ADMIN_API_KEY", "admin")
    
    if api_key != admin_key:
        logger.warning(f"Unauthorized log access attempt from {request.remote_addr}")
        return Response("Unauthorized", status=403)
    
    try:
        # Check for log file in multiple locations
        log_paths = [
            "logs/api_requests.log",
            "api_requests.log",
            os.path.join(os.getcwd(), "logs", "api_requests.log"),
            os.path.join(os.getcwd(), "api_requests.log"),
            "/tmp/api_requests.log"
        ]
        
        # Find first log file that exists
        log_file = None
        for path in log_paths:
            if os.path.exists(path):
                log_file = path
                break
        
        if not log_file:
            # Create log directory if needed
            os.makedirs('logs', exist_ok=True)
            log_file = "logs/api_requests.log"
            with open(log_file, "w") as f:
                f.write("Log file created\n")
        
        # Read the log file
        with open(log_file, "r") as f:
            logs = f.readlines()
        
        # Parse and group logs by request ID
        requests = {}
        request_count = 0
        response_count = 0
        timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})')
        request_pattern = re.compile(r'REQUEST ([0-9a-f-]+): (GET|POST|PUT|DELETE|PATCH) ([^ ]+) - ({.*})')
        response_pattern = re.compile(r'RESPONSE ([0-9a-f-]+): (\d{3}) (\d+)ms - ({.*})')
        level_pattern = re.compile(r' - (\w+) - ')
        
        for log in logs:
            # Skip log entries for admin routes
            if "/admin" in log:
                continue
                
            # Extract timestamp
            timestamp_match = timestamp_pattern.search(log)
            if not timestamp_match:
                continue
                
            timestamp_str = timestamp_match.group(1)
            
            # Extract log level
            level_match = level_pattern.search(log)
            log_level = level_match.group(1) if level_match else "INFO"
            
            # Process requests
            request_match = request_pattern.search(log)
            if request_match:
                request_count += 1
                request_id = request_match.group(1)
                method = request_match.group(2)
                path = request_match.group(3)
                request_data_str = request_match.group(4)
                
                # Parse request data
                try:
                    request_data = json.loads(request_data_str)
                except:
                    request_data = {"error": "Could not parse request data"}
                
                # Store request data
                if request_id not in requests:
                    requests[request_id] = {
                        "request": {
                            "request_id": request_id,
                            "method": method,
                            "path": path,
                            "timestamp": timestamp_str,
                            "level": log_level,
                            "data": request_data,
                            "raw": log.strip()
                        },
                        "response": None
                    }
                else:
                    # Update existing request
                    requests[request_id]["request"] = {
                        "request_id": request_id,
                        "method": method,
                        "path": path,
                        "timestamp": timestamp_str,
                        "level": log_level,
                        "data": request_data,
                        "raw": log.strip()
                    }
            
            # Process responses
            response_match = response_pattern.search(log)
            if response_match:
                response_count += 1
                request_id = response_match.group(1)
                status_code = int(response_match.group(2))
                duration_ms = int(response_match.group(3))
                response_data_str = response_match.group(4)
                
                # Parse response data
                try:
                    response_data = json.loads(response_data_str)
                except:
                    response_data = {"error": "Could not parse response data"}
                
                # Store response data if we have the corresponding request
                if request_id in requests:
                    requests[request_id]["response"] = {
                        "request_id": request_id,
                        "status": status_code,
                        "duration": duration_ms,
                        "timestamp": timestamp_str,
                        "level": log_level,
                        "data": response_data,
                        "raw": log.strip()
                    }
                else:
                    # Create entry with just response
                    requests[request_id] = {
                        "request": None,
                        "response": {
                            "request_id": request_id,
                            "status": status_code,
                            "duration": duration_ms,
                            "timestamp": timestamp_str,
                            "level": log_level,
                            "data": response_data,
                            "raw": log.strip()
                        }
                    }
        
        # Calculate some stats
        def format_file_size(size_bytes):
            if size_bytes < 1024:
                return f"{size_bytes} B"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes / (1024 * 1024):.1f} MB"

        # Get file size
        file_size = format_file_size(os.path.getsize(log_file)) if os.path.exists(log_file) else "Unknown"
        
        # Generate HTML
        html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>API Request Logs</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.5.0/styles/default.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.5.0/highlight.min.js"></script>
    <style>
        .log-entry {
            border-left: 4px solid transparent;
            transition: all 0.2s ease;
        }

        .log-entry:hover {
            background-color: rgba(0, 0, 0, 0.03);
        }

        /* Log severity border colors */
        .log-entry[data-severity="ERROR"] {
            border-left-color: var(--bs-danger);
        }

        .log-entry[data-severity="WARNING"] {
            border-left-color: var(--bs-warning);
        }

        .log-entry[data-severity="INFO"] {
            border-left-color: var(--bs-info);
        }

        /* Log Row Styles */
        .log-row {
            display: flex;
            align-items: center;
            cursor: pointer;
            padding: 8px 0;
        }

        /* Toggle button for expanding details */
        .toggle-details {
            background: none;
            border: none;
            color: var(--bs-secondary);
            cursor: pointer;
            transition: transform 0.2s ease;
            width: 24px;
            height: 24px;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 0;
        }

        .toggle-details:hover {
            color: var(--bs-primary);
        }

        /* Details section styles */
        .log-details {
            background-color: rgba(0, 0, 0, 0.01);
            border-top: 1px solid rgba(0, 0, 0, 0.05);
            padding: 15px;
        }

        /* Tab content container */
        .tab-content {
            padding: 15px;
            background-color: #fff;
            border: 1px solid #dee2e6;
            border-top: 0;
            border-radius: 0 0 0.25rem 0.25rem;
        }

        /* JSON highlighting */
        pre.hljs {
            background-color: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 4px;
        }

        /* Spinner animation for refresh button */
        .spinning {
            animation: spin 1s infinite linear;
        }

        @keyframes spin {
            from {
                transform: rotate(0deg);
            }
            to {
                transform: rotate(360deg);
            }
        }

        /* Ensure the log list container has a minimum height */
        #logsList {
            min-height: 200px;
        }

        /* Make timestamps and IDs slightly muted */
        .timestamp, .request-id {
            color: var(--bs-secondary);
            font-size: 0.85rem;
        }

        /* Path text styling */
        .path {
            font-family: monospace;
            word-break: break-all;
        }

        /* Method and status badges */
        .method-badge, .status-badge, .duration-badge {
            font-size: 0.75rem;
            min-width: 60px;
            text-align: center;
        }

        /* Background colors for log entries based on status */
        .bg-warning-subtle {
            background-color: rgba(255, 193, 7, 0.15);
        }

        .bg-danger-subtle {
            background-color: rgba(220, 53, 69, 0.15);
        }
    </style>
</head>
<body class="container-fluid py-4">
    <h1 class="mb-4">API Request Logs</h1>
    
    <div class="row mb-3">
        <div class="col-12">
            <div class="card shadow-sm">
                <div class="card-body p-3">
                    <div class="row g-2">
                        <!-- Search and filters -->
                        <div class="col-md-8">
                            <div class="d-flex flex-wrap gap-2">
                                <div class="input-group input-group-sm" style="max-width: 300px;">
                                    <span class="input-group-text"><i class="fas fa-search"></i></span>
                                    <input type="text" id="searchInput" class="form-control" placeholder="Search logs...">
                                </div>
                                
                                <select id="endpointFilter" class="form-select form-select-sm" style="max-width: 150px;">
                                    <option value="">All Endpoints</option>
                                </select>
                                
                                <select id="statusFilter" class="form-select form-select-sm" style="max-width: 120px;">
                                    <option value="">All Status</option>
                                    <option value="2">2xx (Success)</option>
                                    <option value="3">3xx (Redirect)</option>
                                    <option value="4">4xx (Client Error)</option>
                                    <option value="5">5xx (Server Error)</option>
                                </select>
                                
                                <select id="timeFilter" class="form-select form-select-sm" style="max-width: 120px;">
                                    <option value="">All Time</option>
                                    <option value="today">Today</option>
                                    <option value="yesterday">Yesterday</option>
                                    <option value="week">This Week</option>
                                </select>
                                
                                <select id="sortBySelect" class="form-select form-select-sm" style="max-width: 120px;">
                                    <option value="timestamp">Sort: Time</option>
                                    <option value="duration">Sort: Duration</option>
                                    <option value="status">Sort: Status</option>
                                    <option value="path">Sort: Endpoint</option>
                                </select>
                                
                                <div class="btn-group btn-group-sm">
                                    <button id="sortAscBtn" type="button" class="btn btn-outline-secondary">
                                        <i class="fas fa-sort-amount-up-alt"></i>
                                    </button>
                                    <button id="sortDescBtn" type="button" class="btn btn-outline-secondary active">
                                        <i class="fas fa-sort-amount-down"></i>
                                    </button>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Stats -->
                        <div class="col-md-4">
                            <div class="d-flex justify-content-end">
                                <div class="d-flex flex-column me-3">
                                    <div class="d-flex align-items-center">
                                        <span class="badge bg-secondary me-1" id="requestCount">0</span>
                                        <small>Requests (24h)</small>
                                    </div>
                                    <div class="d-flex align-items-center">
                                        <span class="badge bg-secondary me-1" id="responseCount">0</span>
                                        <small>Responses (24h)</small>
                                    </div>
                                </div>
                                <div class="d-flex flex-column me-3">
                                    <div class="d-flex align-items-center">
                                        <span class="badge bg-secondary me-1" id="errorCount">0</span>
                                        <small>Errors (24h)</small>
                                    </div>
                                    <div class="d-flex align-items-center">
                                        <span class="badge bg-secondary me-1" id="fileSize">0KB</span>
                                        <small>Log Size</small>
                                    </div>
                                </div>
                                <button id="refreshLogsBtn" class="btn btn-sm btn-secondary">
                                    <i class="fas fa-sync-alt"></i>
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <div id="logsList" class="list-group mb-3">
        <!-- Log entries will be populated here -->
    </div>
    
    <template id="logEntryTemplate">
        <div class="log-entry card mb-2">
            <div class="log-row card-body py-2 px-3">
                <div class="d-flex justify-content-between align-items-center w-100">
                    <div class="d-flex align-items-center flex-wrap">
                        <span class="timestamp me-3" style="width: 120px; display: inline-block;"></span>
                        <span class="method-badge badge me-2"></span>
                        <span class="path text-truncate" style="min-width: 160px; display: inline-block;"></span>
                        <small class="request-id text-muted ms-3"></small>
                        <div class="query-params ms-2 d-inline-flex flex-wrap"></div>
                    </div>
                    <div class="d-flex align-items-center">
                        <small class="duration text-muted me-2" style="font-size: 0.8rem;"></small>
                        <span class="status-code badge me-3"></span>
                        <button class="btn btn-sm btn-outline-secondary replay-request me-2" title="Replay this request">
                            <i class="fas fa-redo-alt"></i>
                        </button>
                        <button class="toggle-details btn btn-sm btn-link p-0">
                            <i class="fas fa-chevron-down"></i>
                        </button>
                    </div>
                </div>
            </div>
            <div class="log-details d-none">
                <div class="card-body pt-0 pb-2 px-3">
                    <ul class="nav nav-tabs" role="tablist">
                        <li class="nav-item">
                            <a class="nav-link active" data-bs-toggle="tab" href="#" role="tab" data-bs-target=".request-tab-content">Request</a>
                        </li>
                        <li class="nav-item">
                            <a class="nav-link" data-bs-toggle="tab" href="#" role="tab" data-bs-target=".response-tab-content">Response</a>
                        </li>
                    </ul>
                    <div class="tab-content">
                        <div class="tab-pane fade show active request-tab-content" role="tabpanel">
                            <!-- Request details will be populated here -->
                        </div>
                        <div class="tab-pane fade response-tab-content" role="tabpanel">
                            <!-- Response details will be populated here -->
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </template>

    <!-- Hidden data element to pass log data to JavaScript -->
    <div id="logData" style="display: none;">{{ logs|safe }}</div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            // DOM elements
            const logsList = document.getElementById('logsList');
            const searchInput = document.getElementById('searchInput');
            const endpointFilter = document.getElementById('endpointFilter');
            const statusFilter = document.getElementById('statusFilter');
            const timeFilter = document.getElementById('timeFilter');
            const sortBySelect = document.getElementById('sortBySelect');
            const sortAscBtn = document.getElementById('sortAscBtn');
            const sortDescBtn = document.getElementById('sortDescBtn');
            const refreshLogsBtn = document.getElementById('refreshLogsBtn');
            const requestCount = document.getElementById('requestCount');
            const responseCount = document.getElementById('responseCount');
            const errorCount = document.getElementById('errorCount');
            const fileSize = document.getElementById('fileSize');
            
            // Initialize logs from the hidden element
            let logs = [];
            try {
                const logData = document.getElementById('logData').textContent;
                logs = JSON.parse(logData);
                if (!Array.isArray(logs)) {
                    console.error('Logs data is not an array:', logs);
                    logs = [];
                }
            } catch (e) {
                console.error('Error parsing logs data:', e);
            }
            
            let filteredLogs = [...logs];
            
            // Current filters state
            let currentFilters = {
                search: '',
                endpoint: '',
                status: '',
                time: '',
                sortBy: 'timestamp',
                sortOrder: 'desc'
            };
            
            // Initialize the application
            initApp();
            
            // Event listeners
            refreshLogsBtn.addEventListener('click', function() {
                window.location.reload();
            });
            
            searchInput.addEventListener('input', function() {
                currentFilters.search = searchInput.value;
                applyFilters();
            });
            
            endpointFilter.addEventListener('change', function() {
                currentFilters.endpoint = endpointFilter.value;
                applyFilters();
            });
            
            statusFilter.addEventListener('change', function() {
                currentFilters.status = statusFilter.value;
                applyFilters();
            });
            
            timeFilter.addEventListener('change', function() {
                currentFilters.time = timeFilter.value;
                applyFilters();
            });
            
            sortBySelect.addEventListener('change', function() {
                currentFilters.sortBy = sortBySelect.value;
                applyFilters();
            });
            
            sortAscBtn.addEventListener('click', function() {
                sortAscBtn.classList.add('active');
                sortDescBtn.classList.remove('active');
                currentFilters.sortOrder = 'asc';
                applyFilters();
            });
            
            sortDescBtn.addEventListener('click', function() {
                sortDescBtn.classList.add('active');
                sortAscBtn.classList.remove('active');
                currentFilters.sortOrder = 'desc';
                applyFilters();
            });
            
            // App initialization function
            function initApp() {
                // Populate endpoint filter
                populateEndpointFilter();
                
                // Calculate and display stats
                updateStats();
                
                // Apply initial filters
                applyFilters();
            }
            
            // Populate endpoint filter from available logs
            function populateEndpointFilter() {
                const endpoints = new Set();
                
                // Extract all unique endpoints
                logs.forEach(pair => {
                    if (pair.request && pair.request.path) {
                        // Extract base endpoint without query params
                        const path = pair.request.path.split('?')[0];
                        endpoints.add(path);
                    }
                });
                
                // Clear existing options except the first one
                while (endpointFilter.options.length > 1) {
                    endpointFilter.remove(1);
                }
                
                // Add options for each endpoint
                Array.from(endpoints).sort().forEach(endpoint => {
                    const option = document.createElement('option');
                    option.value = endpoint;
                    option.textContent = endpoint;
                    endpointFilter.appendChild(option);
                });
            }
            
            // Update stats display
            function updateStats() {
                // Get current time and 24 hours ago
                const now = new Date();
                const yesterday = new Date(now);
                yesterday.setDate(yesterday.getDate() - 1);
                
                // Filter logs for the past 24 hours
                const recentLogs = logs.filter(pair => {
                    const timestamp = (pair.request && pair.request.timestamp) || 
                                     (pair.response && pair.response.timestamp);
                    
                    if (!timestamp) return false;
                    
                    const logDate = new Date(timestamp.replace(',', '.'));
                    return logDate >= yesterday;
                });
                
                // Request count (past 24 hours)
                const recentRequests = recentLogs.filter(pair => pair.request).length;
                requestCount.textContent = recentRequests;
                
                // Response count (past 24 hours)
                const recentResponses = recentLogs.filter(pair => pair.response).length;
                responseCount.textContent = recentResponses;
                
                // Error count (4xx and 5xx responses in past 24 hours)
                const recentErrors = recentLogs.filter(pair => 
                    pair.response && 
                    pair.response.status && 
                    pair.response.status >= 400
                ).length;
                errorCount.textContent = recentErrors;
                
                // File size (total)
                fileSize.textContent = document.getElementById('logFileSize') ? 
                    document.getElementById('logFileSize').textContent : '0KB';
            }
            
            // Apply filters and sort to logs
            function applyFilters() {
                // Filter the log pairs
                filteredLogs = logs.filter(pair => {
                    // Check if endpoint matches filter
                    if (currentFilters.endpoint) {
                        const requestPath = pair.request ? pair.request.path : '';
                        if (!requestPath.startsWith(currentFilters.endpoint)) {
                            return false;
                        }
                    }
                    
                    // Check if status matches filter
                    if (currentFilters.status) {
                        const status = pair.response ? pair.response.status : 0;
                        const statusPrefix = currentFilters.status.charAt(0);
                        if (!status || status.toString().charAt(0) !== statusPrefix) {
                            return false;
                        }
                    }
                    
                    // Check if time matches filter
                    if (currentFilters.time) {
                        const timestamp = (pair.request && pair.request.timestamp) || 
                                         (pair.response && pair.response.timestamp);
                        
                        if (!timestamp) return false;
                        
                        const logDate = new Date(timestamp.replace(',', '.'));
                        const now = new Date();
                        const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
                        const yesterday = new Date(today);
                        yesterday.setDate(yesterday.getDate() - 1);
                        const weekStart = new Date(today);
                        weekStart.setDate(weekStart.getDate() - weekStart.getDay());
                        
                        if (currentFilters.time === 'today' && logDate < today) {
                            return false;
                        } else if (currentFilters.time === 'yesterday' && 
                                  (logDate < yesterday || logDate >= today)) {
                            return false;
                        } else if (currentFilters.time === 'week' && logDate < weekStart) {
                            return false;
                        }
                    }
                    
                    // Check if any part of the pair matches search query
                    if (currentFilters.search) {
                        const searchQuery = currentFilters.search.toLowerCase();
                        const requestMatches = logMatchesSearch(pair.request, searchQuery);
                        const responseMatches = logMatchesSearch(pair.response, searchQuery);
                        
                        if (!requestMatches && !responseMatches) {
                            return false;
                        }
                    }
                    
                    return true;
                });
                
                // Sort the pairs
                filteredLogs = sortLogPairs(filteredLogs, currentFilters.sortBy, currentFilters.sortOrder);
                
                // Update UI
                renderLogs(filteredLogs);
            }
            
            // Check if a log matches the search query
            function logMatchesSearch(log, searchQuery) {
                if (!log) return false;
                
                searchQuery = searchQuery.toLowerCase();
                const searchableFields = ['raw', 'path', 'method', 'request_id'];
                
                // Check if any of the searchable fields contain the search query
                for (const field of searchableFields) {
                    if (log[field] && String(log[field]).toLowerCase().includes(searchQuery)) {
                        return true;
                    }
                }
                
                // Search in data if it exists
                if (log.data) {
                    const dataStr = JSON.stringify(log.data).toLowerCase();
                    if (dataStr.includes(searchQuery)) {
                        return true;
                    }
                }
                
                return false;
            }
            
            // Sort log pairs by specified field and order
            function sortLogPairs(pairs, sortBy, sortOrder) {
                return [...pairs].sort((a, b) => {
                    let valueA, valueB;
                    
                    // For timestamp, use the request timestamp if available, otherwise response
                    if (sortBy === 'timestamp') {
                        valueA = (a.request && a.request.timestamp) || (a.response && a.response.timestamp) || '';
                        valueB = (b.request && b.request.timestamp) || (b.response && b.response.timestamp) || '';
                    } 
                    // For duration, use the response duration
                    else if (sortBy === 'duration') {
                        valueA = (a.response && a.response.duration) || 0;
                        valueB = (b.response && b.response.duration) || 0;
                    } 
                    // For status, use the response status
                    else if (sortBy === 'status') {
                        valueA = (a.response && a.response.status) || 0;
                        valueB = (b.response && b.response.status) || 0;
                    } 
                    // For level, use the most severe level between request and response
                    else if (sortBy === 'level') {
                        const levelPriority = { 'ERROR': 3, 'WARNING': 2, 'INFO': 1, 'DEBUG': 0 };
                        const aRequestLevel = a.request ? a.request.level : '';
                        const aResponseLevel = a.response ? a.response.level : '';
                        const bRequestLevel = b.request ? b.request.level : '';
                        const bResponseLevel = b.response ? b.response.level : '';
                        
                        valueA = Math.max(
                            levelPriority[aRequestLevel] || 0, 
                            levelPriority[aResponseLevel] || 0
                        );
                        valueB = Math.max(
                            levelPriority[bRequestLevel] || 0, 
                            levelPriority[bResponseLevel] || 0
                        );
                    } 
                    else {
                        // For other fields, try request first, then response
                        valueA = (a.request && a.request[sortBy]) || (a.response && a.response[sortBy]) || '';
                        valueB = (b.request && b.request[sortBy]) || (b.response && b.response[sortBy]) || '';
                    }
                    
                    // Compare values
                    if (valueA < valueB) {
                        return sortOrder === 'asc' ? -1 : 1;
                    }
                    if (valueA > valueB) {
                        return sortOrder === 'asc' ? 1 : -1;
                    }
                    return 0;
                });
            }
            
            // Render logs to the UI
            function renderLogs(logPairs) {
                // Clear existing logs
                logsList.innerHTML = '';
                
                // Show message if no logs
                if (logPairs.length === 0) {
                    const emptyMessage = document.createElement('div');
                    emptyMessage.className = 'list-group-item text-center text-muted';
                    emptyMessage.innerHTML = '<i class="fas fa-info-circle me-2"></i>No logs match your filters';
                    logsList.appendChild(emptyMessage);
                    return;
                }
                
                // Add log pairs to the list
                logPairs.forEach(pair => {
                    try {
                        const logEntry = createLogPairElement(pair);
                        if (logEntry) {
                            logsList.appendChild(logEntry);
                        } else {
                            console.error('Failed to create log entry for pair:', pair);
                        }
                    } catch (error) {
                        console.error('Error creating log entry:', error, pair);
                    }
                });
                
                // Debug output
                console.log(`Rendered ${logPairs.length} log entries`);
            }
            
            // Create a combined log entry element for request-response pair
            function createLogPairElement(pair) {
                const template = document.getElementById('logEntryTemplate');
                const logEntry = document.importNode(template.content, true).querySelector('.log-entry');
                
                // Extract request and response for convenience
                const request = pair.request;
                const response = pair.response;
                
                // Use request ID as the data-log-id and also to show in the UI
                const requestId = request ? request.request_id : (response ? response.request_id : 'unknown');
                logEntry.dataset.logId = requestId;
                
                // Set request ID in UI
                const requestIdElement = logEntry.querySelector('.request-id');
                requestIdElement.textContent = requestId;
                requestIdElement.title = requestId; // Show full ID on hover
                
                // Set timestamp from request
                const timestamp = logEntry.querySelector('.timestamp');
                if (request) {
                    timestamp.textContent = formatTimestamp(request.timestamp);
                } else if (response) {
                    timestamp.textContent = formatTimestamp(response.timestamp);
                }
                
                // Set method and path from request
                const methodBadge = logEntry.querySelector('.method-badge');
                const path = logEntry.querySelector('.path');
                
                if (request) {
                    methodBadge.textContent = request.method;
                    methodBadge.classList.add(getMethodClass(request.method));
                    path.textContent = request.path;
                } else {
                    methodBadge.style.display = 'none';
                    path.textContent = 'Unknown Path';
                }
                
                // Add query parameter tags if available
                const paramsContainer = logEntry.querySelector('.query-params');
                if (request && request.data && request.data.query_params) {
                    const queryParams = request.data.query_params;
                    paramsContainer.innerHTML = ''; // Clear any existing content
                    
                    // Create a tag for each query parameter
                    Object.entries(queryParams).forEach(([key, value]) => {
                        const paramTag = document.createElement('span');
                        paramTag.className = 'badge me-1';
                        paramTag.style.fontSize = '0.7rem';
                        paramTag.style.fontWeight = 'normal';
                        paramTag.style.backgroundColor = '#e6f2ff'; // Light blue background
                        paramTag.style.color = '#0066cc'; // Darker blue text
                        paramTag.style.border = '1px solid #cce0ff'; // Light blue border
                        paramTag.textContent = `${key}: ${value}`;
                        paramsContainer.appendChild(paramTag);
                    });
                    
                    paramsContainer.style.display = 'inline-flex';
                } else {
                    paramsContainer.style.display = 'none';
                }
                
                // Set response status and duration if available
                const statusElement = logEntry.querySelector('.status-code');
                const durationElement = logEntry.querySelector('.duration');
                
                if (response) {
                    statusElement.textContent = response.status;
                    statusElement.classList.add(getStatusClass(response.status));
                    durationElement.textContent = `${response.duration}ms`;
                } else {
                    statusElement.textContent = '—';
                    durationElement.textContent = '—';
                }
                
                // Set up replay button
                const replayButton = logEntry.querySelector('.replay-request');
                if (request) {
                    replayButton.addEventListener('click', function(e) {
                        e.stopPropagation();
                        replayRequest(request);
                    });
                } else {
                    replayButton.style.display = 'none';
                }
                
                // Set up details toggle
                const toggleButton = logEntry.querySelector('.toggle-details');
                const detailsSection = logEntry.querySelector('.log-details');
                
                // Make the entire row clickable (except the toggle button and replay button)
                const logRow = logEntry.querySelector('.log-row');
                logRow.addEventListener('click', (event) => {
                    // Only trigger if the click wasn't on the toggle button or replay button
                    if (!event.target.closest('.toggle-details') && !event.target.closest('.replay-request')) {
                        toggleButton.click();
                    }
                });
                
                toggleButton.addEventListener('click', (event) => {
                    event.stopPropagation();
                    const isHidden = detailsSection.classList.contains('d-none');
                    
                    if (isHidden) {
                        detailsSection.classList.remove('d-none');
                        toggleButton.innerHTML = '<i class="fas fa-chevron-up"></i>';
                        
                        // Load details if they haven't been loaded yet
                        const requestTab = detailsSection.querySelector('.request-tab-content');
                        const responseTab = detailsSection.querySelector('.response-tab-content');
                        
                        if (requestTab.childElementCount === 0 && responseTab.childElementCount === 0) {
                            loadPairDetails(pair, requestTab, responseTab);
                        }
                    } else {
                        detailsSection.classList.add('d-none');
                        toggleButton.innerHTML = '<i class="fas fa-chevron-down"></i>';
                    }
                });
                
                return logEntry;
            }
            
            // Function to replay a request
            function replayRequest(request) {
                if (!request || !request.method || !request.path) {
                    alert('Cannot replay request: Missing required information');
                    return;
                }
                
                // Create confirmation dialog
                if (!confirm(`Replay this ${request.method} request to ${request.path}?`)) {
                    return;
                }
                
                // Show loading indicator
                const loadingToast = document.createElement('div');
                loadingToast.className = 'position-fixed bottom-0 end-0 p-3';
                loadingToast.style.zIndex = '5000';
                loadingToast.innerHTML = `
                    <div class="toast show" role="alert">
                        <div class="toast-header">
                            <strong class="me-auto">Replaying Request</strong>
                            <button type="button" class="btn-close" data-bs-dismiss="toast"></button>
                        </div>
                        <div class="toast-body">
                            Sending ${request.method} to ${request.path}...
                        </div>
                    </div>
                `;
                document.body.appendChild(loadingToast);
                
                // Prepare the fetch options
                const options = {
                    method: request.method,
                    headers: {
                        'Content-Type': 'application/json'
                    }
                };
                
                // Add body for non-GET requests
                if (request.method !== 'GET' && request.data) {
                    options.body = JSON.stringify(request.data);
                }
                
                // Send the request
                fetch(request.path, options)
                    .then(response => {
                        return response.json().then(data => {
                            return {
                                status: response.status,
                                data: data
                            };
                        }).catch(() => {
                            return {
                                status: response.status,
                                data: null
                            };
                        });
                    })
                    .then(result => {
                        // Remove loading toast
                        document.body.removeChild(loadingToast);
                        
                        // Show success toast
                        const successToast = document.createElement('div');
                        successToast.className = 'position-fixed bottom-0 end-0 p-3';
                        successToast.style.zIndex = '5000';
                        successToast.innerHTML = `
                            <div class="toast show" role="alert">
                                <div class="toast-header">
                                    <strong class="me-auto">Request Completed</strong>
                                    <button type="button" class="btn-close" data-bs-dismiss="toast"></button>
                                </div>
                                <div class="toast-body">
                                    Response: ${result.status} ${result.status < 400 ? '✅' : '❌'}<br>
                                    <small>Refresh the page to see the new log entry</small>
                                </div>
                            </div>
                        `;
                        document.body.appendChild(successToast);
                        
                        // Auto-remove toast after 5 seconds
                        setTimeout(() => {
                            if (document.body.contains(successToast)) {
                                document.body.removeChild(successToast);
                            }
                        }, 5000);
                    })
                    .catch(error => {
                        // Remove loading toast
                        document.body.removeChild(loadingToast);
                        
                        // Show error toast
                        const errorToast = document.createElement('div');
                        errorToast.className = 'position-fixed bottom-0 end-0 p-3';
                        errorToast.style.zIndex = '5000';
                        errorToast.innerHTML = `
                            <div class="toast show bg-danger text-white" role="alert">
                                <div class="toast-header bg-danger text-white">
                                    <strong class="me-auto">Error</strong>
                                    <button type="button" class="btn-close" data-bs-dismiss="toast"></button>
                                </div>
                                <div class="toast-body">
                                    Failed to replay request: ${error.message}
                                </div>
                            </div>
                        `;
                        document.body.appendChild(errorToast);
                        
                        // Auto-remove toast after 5 seconds
                        setTimeout(() => {
                            if (document.body.contains(errorToast)) {
                                document.body.removeChild(errorToast);
                            }
                        }, 5000);
                    });
            }
            
            // Get the highest severity level between two levels
            function getHighestLevel(level1, level2) {
                const levelPriority = { 'ERROR': 3, 'WARNING': 2, 'INFO': 1, 'DEBUG': 0 };
                
                if (!level1 && !level2) return 'UNKNOWN';
                if (!level1) return level2;
                if (!level2) return level1;
                
                return levelPriority[level1] >= levelPriority[level2] ? level1 : level2;
            }
            
            // Load details for a request-response pair
            function loadPairDetails(pair, requestTab, responseTab) {
                const request = pair.request;
                const response = pair.response;
                
                // Find the parent tab elements
                const detailsSection = requestTab.closest('.log-details');
                const responseNavTab = detailsSection.querySelector('.nav-link[data-bs-target=".response-tab-content"]');
                const requestNavTab = detailsSection.querySelector('.nav-link[data-bs-target=".request-tab-content"]');
                
                // Check if we have a response and update the response tab's state
                if (!response) {
                    // Disable the response tab if there's no response
                    responseNavTab.classList.add('disabled');
                    responseNavTab.classList.add('text-muted');
                    responseNavTab.style.opacity = '0.6';
                    responseNavTab.title = 'No response data available';
                } else {
                    // Enable response tab and add click handlers
                    responseNavTab.addEventListener('click', function() {
                        requestTab.classList.remove('show', 'active');
                        responseTab.classList.add('show', 'active');
                        requestNavTab.classList.remove('active');
                        responseNavTab.classList.add('active');
                    });
                    
                    requestNavTab.addEventListener('click', function() {
                        responseTab.classList.remove('show', 'active');
                        requestTab.classList.add('show', 'active');
                        responseNavTab.classList.remove('active');
                        requestNavTab.classList.add('active');
                    });
                }
                
                // Load response details
                if (response) {
                    // Response raw data section
                    const responseRawSection = document.createElement('div');
                    responseRawSection.className = 'mb-2';
                    
                    const responseRawTitle = document.createElement('h6');
                    responseRawTitle.className = 'text-muted mb-1';
                    responseRawTitle.innerHTML = '<i class="fas fa-code me-2"></i>Raw Response';
                    responseRawSection.appendChild(responseRawTitle);
                    
                    const responseCodeBlock = document.createElement('pre');
                    responseCodeBlock.className = 'p-2 rounded';
                    responseCodeBlock.style.maxHeight = '200px';
                    responseCodeBlock.style.overflow = 'auto';
                    
                    const responseCode = document.createElement('code');
                    responseCode.textContent = response.raw;
                    responseCodeBlock.appendChild(responseCode);
                    responseRawSection.appendChild(responseCodeBlock);
                    
                    responseTab.appendChild(responseRawSection);
                    
                    // Response data section
                    if (response.data) {
                        const responseDataSection = document.createElement('div');
                        responseDataSection.className = 'mb-2';
                        
                        const responseDataTitle = document.createElement('h6');
                        responseDataTitle.className = 'text-muted mb-1';
                        responseDataTitle.innerHTML = '<i class="fas fa-arrow-left me-2"></i>Response Data';
                        responseDataSection.appendChild(responseDataTitle);
                        
                        const responseJsonBlock = document.createElement('pre');
                        responseJsonBlock.className = 'json p-2 rounded';
                        responseJsonBlock.style.maxHeight = '300px';
                        responseJsonBlock.style.overflow = 'auto';
                        
                        const responseJsonCode = document.createElement('code');
                        responseJsonCode.className = 'language-json';
                        responseJsonCode.textContent = JSON.stringify(response.data, null, 2);
                        responseJsonBlock.appendChild(responseJsonCode);
                        responseDataSection.appendChild(responseJsonBlock);
                        
                        responseTab.appendChild(responseDataSection);
                    }
                }
                
                // Request raw data section
                const requestRawSection = document.createElement('div');
                requestRawSection.className = 'mb-2';
                
                const requestRawTitle = document.createElement('h6');
                requestRawTitle.className = 'text-muted mb-1';
                requestRawTitle.innerHTML = '<i class="fas fa-code me-2"></i>Raw Request';
                requestRawSection.appendChild(requestRawTitle);
                
                const requestCodeBlock = document.createElement('pre');
                requestCodeBlock.className = 'p-2 rounded';
                requestCodeBlock.style.maxHeight = '200px';
                requestCodeBlock.style.overflow = 'auto';
                
                const requestCode = document.createElement('code');
                requestCode.textContent = request.raw || JSON.stringify(request, null, 2);
                requestCodeBlock.appendChild(requestCode);
                requestRawSection.appendChild(requestCodeBlock);
                
                requestTab.appendChild(requestRawSection);
                
                // Request data section
                if (request.data) {
                    const requestDataSection = document.createElement('div');
                    requestDataSection.className = 'mb-2';
                    
                    const requestDataTitle = document.createElement('h6');
                    requestDataTitle.className = 'text-muted mb-1';
                    requestDataTitle.innerHTML = '<i class="fas fa-arrow-right me-2"></i>Request Data';
                    requestDataSection.appendChild(requestDataTitle);
                    
                    const requestJsonBlock = document.createElement('pre');
                    requestJsonBlock.className = 'json p-2 rounded';
                    requestJsonBlock.style.maxHeight = '300px';
                    requestJsonBlock.style.overflow = 'auto';
                    
                    const requestJsonCode = document.createElement('code');
                    requestJsonCode.className = 'language-json';
                    try {
                        const jsonData = typeof request.data === 'string' ? JSON.parse(request.data) : request.data;
                        requestJsonCode.textContent = JSON.stringify(jsonData, null, 2);
                    } catch (e) {
                        requestJsonCode.textContent = String(request.data);
                    }
                    requestJsonBlock.appendChild(requestJsonCode);
                    requestDataSection.appendChild(requestJsonBlock);
                    
                    requestTab.appendChild(requestDataSection);
                }
                
                // Apply syntax highlighting
                document.querySelectorAll('pre code').forEach((block) => {
                    hljs.highlightElement(block);
                });
            }
            
            // Get CSS class for HTTP method
            function getMethodClass(method) {
                if (!method) return 'bg-secondary';
                
                switch (method.toUpperCase()) {
                    case 'GET':
                        return 'bg-success';
                    case 'POST':
                        return 'bg-primary';
                    case 'PUT':
                        return 'bg-info';
                    case 'DELETE':
                        return 'bg-danger';
                    case 'PATCH':
                        return 'bg-warning';
                    default:
                        return 'bg-secondary';
                }
            }
            
            // Get status code class
            function getStatusClass(status) {
                if (!status) return 'text-secondary';
                
                if (status < 300) return 'text-success';
                if (status < 400) return 'text-info';
                if (status < 500) return 'text-warning';
                return 'text-danger';
            }
            
            // Format timestamp for display as "Mar 3, 3:31pm"
            function formatTimestamp(timestamp) {
                if (!timestamp) return '';
                
                try {
                    // Parse the timestamp (2025-04-07 17:43:35,330)
                    // First replace the comma with a period for better parsing
                    const fixedTimestamp = timestamp.replace(',', '.');
                    const date = new Date(fixedTimestamp);
                    
                    if (isNaN(date.getTime())) {
                        return timestamp; // Return original if parsing fails
                    }
                    
                    // Format month
                    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                    const month = months[date.getMonth()];
                    
                    // Format day
                    const day = date.getDate();
                    
                    // Format time (hour:minute am/pm)
                    let hours = date.getHours();
                    const ampm = hours >= 12 ? 'pm' : 'am';
                    hours = hours % 12;
                    hours = hours ? hours : 12; // Hour 0 should be 12
                    const minutes = date.getMinutes().toString().padStart(2, '0');
                    
                    // Format into user-requested format
                    return `${month} ${day}, ${hours}:${minutes}${ampm}`;
                } catch (e) {
                    console.error('Error formatting timestamp:', e);
                    return timestamp; // Return original if any error
                }
            }
        });
    </script>

    <!-- Add a hidden element for file size -->
    <div id="logFileSize" style="display: none;">{file_size}</div>
</body>
</html>
"""
        
        # Convert requests dictionary to a list for easier handling in JavaScript
        log_pairs = list(requests.values())

        return render_template_string(html, logs=json.dumps(log_pairs))
        
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error reading logs: {error_message}")
        import traceback
        return f"""
        <html>
        <body>
            <h1>Error reading logs</h1>
            <p>{error_message}</p>
            <p>Current working directory: {os.getcwd()}</p>
            <p>Log files in current directory:</p>
            <ul>
                {"".join(f"<li>{f}</li>" for f in os.listdir(".") if f.endswith(".log"))}
            </ul>
            <p>Exception traceback:</p>
            <pre>{traceback.format_exc()}</pre>
        </body>
        </html>
        """
