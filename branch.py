import grpc
import example_pb2
import example_pb2_grpc
from concurrent import futures
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Branch(example_pb2_grpc.RPCServicer):
    
    def __init__(self, id, balance, branches):
        self.id = id
        self.balance = balance
        self.branches = branches
        self.stubList = []
        self.recvMsg = []
        
        # Create stubs for inter-branch communication
        for branch_id in self.branches:
            if branch_id != self.id:
                channel = grpc.insecure_channel(f'localhost:{50000 + branch_id}')
                stub = example_pb2_grpc.RPCStub(channel)
                self.stubList.append(stub)

    def MsgDelivery(self, request, context):
        """Handles incoming requests and directs them to the appropriate interface."""
        self.recvMsg.append(request)
        
        if request.interface == "query":
            return self.Query(request)
        elif request.interface == "deposit":
            return self.Deposit(request)
        elif request.interface == "withdraw":
            return self.Withdraw(request)
        elif request.interface == "propagate_deposit":
            return self.Propagate_Deposit(request)
        elif request.interface == "propagate_withdraw":
            return self.Propagate_Withdraw(request)
        else:
            logger.error(f"Invalid interface requested: {request.interface}")
            return example_pb2.Response(interface="error", result="Invalid interface")

    def Query(self, request):
        """Handles balance query requests."""
        return example_pb2.Response(interface="query", balance=self.balance)

    def Deposit(self, request):
        """Handles deposit requests and propagates the deposit to other branches."""
        if request.money < 0:
            logger.error("Deposit amount cannot be negative.")
            return example_pb2.Response(interface="deposit", result="fail")

        # Update balance
        self.balance += request.money
        self.Propagate_To_Branches("propagate_deposit", request.money)
        logger.info(f"Branch {self.id} balance after deposit: {self.balance}")
        return example_pb2.Response(interface="deposit", result="success")

    def Withdraw(self, request):
        """Handles withdrawal requests and propagates the withdrawal to other branches."""
        if request.money < 0:
            logger.error("Withdrawal amount cannot be negative.")
            return example_pb2.Response(interface="withdraw", result="fail")

        if self.balance >= request.money:
            self.balance -= request.money
            self.Propagate_To_Branches("propagate_withdraw", request.money)
            logger.info(f"Branch {self.id} balance after withdrawal: {self.balance}")
            return example_pb2.Response(interface="withdraw", result="success")
        else:
            return example_pb2.Response(interface="withdraw", result="fail")

    def Propagate_Deposit(self, request):
        """Receives deposit propagation from other branches and updates balance."""
        self.balance += request.money
        logger.info(f"Branch {self.id} balance after propagated deposit: {self.balance}")
        return example_pb2.Response(interface="propagate_deposit", result="success")

    def Propagate_Withdraw(self, request):
        """Receives withdrawal propagation from other branches and updates balance."""
        self.balance -= request.money
        logger.info(f"Branch {self.id} balance after propagated withdrawal: {self.balance}")
        return example_pb2.Response(interface="propagate_withdraw", result="success")

    def Propagate_To_Branches(self, interface, money):
        """Propagates deposit or withdrawal actions to all other branches with acknowledgment."""
        for stub in self.stubList:
            try:
                request = example_pb2.Request(interface=interface, money=money)
                response = stub.MsgDelivery(request)
                if response.result != "success":
                    logger.warning(f"Failed to propagate {interface} to branch.")
                time.sleep(0.1)  # Small delay to allow propagation
            except grpc.RpcError as e:
                logger.error(f"Error propagating to branch: {e.details()}")
