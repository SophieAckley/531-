import json
import time
from concurrent import futures
import grpc
import example_pb2
import example_pb2_grpc
from branch import Branch, serve
from customer import Customer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_branch(branch_data, branches):
    """Initializes and runs a branch process."""
    branch = Branch(branch_data['id'], branch_data['balance'], [b['id'] for b in branches])
    serve(branch)

def run_customer(customer_data):
    """Executes events for a customer process and returns the result."""
    customer = Customer(customer_data['id'], customer_data['events'])
    return customer.executeEvents()

if __name__ == '__main__':
    # Read input file
    try:
        with open('input.json', 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        logger.error("input.json file not found.")
        exit(1)
    except json.JSONDecodeError:
        logger.error("Error decoding input.json. Please check the file format.")
        exit(1)

    # Separate branch and customer data
    branches = [item for item in data if item['type'] == 'branch']
    customers = [item for item in data if item['type'] == 'customer']

    # Start branch processes
    executor = futures.ThreadPoolExecutor(max_workers=len(branches))
    branch_futures = [executor.submit(run_branch, branch, branches) for branch in branches]
    logger.info("Branch processes started.")

    # Wait for branches to initialize (add an appropriate delay or signal handling)
    time.sleep(2)  # Consider using a more reliable synchronization method

    # Execute customer events in sequence
    output = []
    for customer in customers:
        result = run_customer(customer)
        output.append({"id": customer['id'], "recv": result})
        # Add short delay between customers to ensure sequential execution
        time.sleep(0.5)

    # Write output file
    try:
        with open('output.json', 'w') as f:
            json.dump(output, f, indent=2)
        logger.info("Output successfully written to output.json")
    except IOError as e:
        logger.error(f"Failed to write output.json: {e}")

    # Wait for all branch processes to complete
    for future in branch_futures:
        future.result()
    
    # Shutdown the executor
    executor.shutdown(wait=True)
    logger.info("All processes completed.")
