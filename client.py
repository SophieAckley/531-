import json
import time
import grpc
import banks_pb2
import banks_pb2_grpc
from customer import Customer

def run_customer(customer_data):
    customer = Customer(customer_data['id'], customer_data['events'])
    return customer.executeEvents()

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python client.py input.json")
        sys.exit(1)

    input_file = sys.argv[1]
    
    with open(input_file, 'r') as f:
        data = json.load(f)

    customers = [item for item in data if item['type'] == 'customer']
    output = []

    for customer in customers:
        result = run_customer(customer)
        output.append({"id": customer['id'], "recv": result})
        time.sleep(0.5)  # Short delay between customers

    with open('output.json', 'w') as f:
        json.dump(output, f, indent=2)

    print("All customer processes completed. Output written to output.json")
