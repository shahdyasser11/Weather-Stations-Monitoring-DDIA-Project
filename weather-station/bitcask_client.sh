#!/bin/bash

# our centeral station is listening on port 8080
SERVER_URL="http://localhost:8080"


#  to view All Data (-view-all)
if [ "$1" == "-view-all" ]; then
    # Generate the current Unix timestamp
    TIMESTAMP=$(date +%s)
    FILENAME="${TIMESTAMP}.csv"
    
    # Fetch data and save it to the CSV file
    curl -s "${SERVER_URL}/view-all" > "$FILENAME"
    echo "Success: Exported all database keys to $FILENAME"

#  to view Specific Key (-view-key)
elif [ "$1" == "-view-key" ]; then
    # Ensure they provided a key as the second argument
    if [ -z "$2" ]; then
        echo "Error: Please provide a key. Example: ./bitcask_client.sh -view-key 1"
        exit 1
    fi
    
    # Fetch and print the exact value to stdout
    curl -s "${SERVER_URL}/view-key?id=$2"
    echo "" 

# to test the concurrent Stress Test (-t=100)

elif [[ "$1" == -t=* ]]; then
    # Extract the number of threads from the argument (e.g., "100")
    THREADS=${1#-t=}
    TIMESTAMP=$(date +%s)
    
    echo "Starting stress test with $THREADS concurrent threads..."

    # Loop and spawn background processes
    for (( i=1; i<=THREADS; i++ ))
    do
        (
            FILENAME="${TIMESTAMP}_thread_${i}.csv"
            curl -s "${SERVER_URL}/view-all" > "$FILENAME"
        ) & # The '&' pushes this task to the background, running them all simultaneously
    done

    # Wait for all background threads to finish before exiting
    wait
    echo "Success: All $THREADS threads finished. Generated $THREADS CSV files."

# to clean the csv file

elif [ "$1" == "-clean" ]; then
    echo "Cleaning up generated CSV files..."
    
    # Check if there are any CSV files to delete to avoid error messages
    if ls *.csv 1> /dev/null 2>&1; then
        rm *.csv
        echo "Success: All CSV files have been removed from the directory."
    else
        echo "No CSV files found to clean."
    fi
# Error Handling: Invalid Command
else
    echo "Invalid command. Usage:"
    echo "  ./bitcask_client.sh -view-all"
    echo "  ./bitcask_client.sh -view-key <key>"
    echo "  ./bitcask_client.sh -t=<number_of_threads>"
    echo "  ./bitcask_client.sh -clean"
fi