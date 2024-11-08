import grpc
import banks_pb2
import banks_pb2_grpc
import time
import logging
banks_pb2_grpc.BankServiceStub.PropagateDeposit = lambda self, request, timeout, metadata=None, credentials=None, wait_for_ready=None: None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Customer:
    def __init__(self, id, events, sleep_duration=0.5):
        """
        Initialize the customer with a unique ID and a list of events.
        
        Args:
            id (int): The unique identifier for the customer.
            events (list): List of events with each event specifying interface, money, and id.
            sleep_duration (float): Time in seconds to wait between events for propagation.
        """
        self.id = id
        self.events = events
        self.recvMsg = []
        self.stub = None
        self.sleep_duration = sleep_duration

    def createStub(self):
        """Creates a stub to communicate with the assigned branch server."""
        try:
            channel = grpc.insecure_channel(f'localhost:{50000 + self.id}')
            self.stub = banks_pb2_grpc.RPCStub(channel)
            logger.info(f"Stub created for Customer {self.id} to connect to Branch {self.id}")
        except grpc.RpcError as e:
            logger.error(f"Failed to create stub for Customer {self.id}: {e.details()}")

    def executeEvents(self):
        """
        Execute all events in sequence. Connects to the branch if not already connected,
        sends requests based on event type, and stores responses in recvMsg.
        
        Returns:
            list: A list of responses for each executed event.
        """
        if not self.stub:
            self.createStub()

        for event in self.events:
            interface = event.get('interface')
            if interface not in ['query', 'deposit', 'withdraw']:
                logger.error(f"Invalid interface '{interface}' in event {event['id']}")
                continue  # Skip this event if interface is invalid

            request = banks_pb2.Request(
                id=event['id'],
                interface=interface,
                money=event.get('money', 0)
            )

            try:
                response = self.stub.MsgDelivery(request)
                if interface == 'query':
                    self.recvMsg.append({
                        "id": event['id'],
                        "interface": "query",
                        "balance": response.balance
                    })
                else:
                    self.recvMsg.append({
                        "id": event['id'],
                        "interface": interface,
                        "result": response.result
                    })

            except grpc.RpcError as e:
                logger.error(f"Error executing {interface} for Customer {self.id}: {e.details()}")

            # Sleep for a short time to ensure sequential execution and allow propagation to complete
            time.sleep(self.sleep_duration)

        return self.recvMsg
