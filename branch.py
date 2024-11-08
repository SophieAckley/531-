import grpc

import banks_pb2_grpc
import banks_pb2
from concurrent import futures
import logging
import time
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Branch(banks_pb2_grpc.BankServiceServicer):
    """Implements the BankServiceServicer interface."""
    
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
                stub = banks_pb2_grpc.RPCStub(channel)
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
            return banks_pb2.Response(interface="error", result="Invalid interface")

    #def Query(self, request):
        """Handles balance query requests."""
        return banks_pb2.Response(interface="query", balance=self.balance)
    
    def Query(self, request, context):
        return banks_pb2.QueryResponse(balance=self.balance)

    

    def Deposit(self, request):
        """Handles deposit requests and propagates the deposit to other branches."""
        if request.money < 0:
            logger.error("Deposit amount cannot be negative.")
            return banks_pb2.Response(interface="deposit", result="fail")

        # Update balance
        self.balance += request.money
        self.Propagate_To_Branches("propagate_deposit", request.money)
        return banks_pb2.Response(interface="deposit", result="success")

    def Withdraw(self, request):
        """Handles withdrawal requests and propagates the withdrawal to other branches."""
        if request.money < 0:
            logger.error("Withdrawal amount cannot be negative.")
            return banks_pb2.Response(interface="withdraw", result="fail")

        if self.balance >= request.money:
            self.balance -= request.money
            self.Propagate_To_Branches("propagate_withdraw", request.money)
            return banks_pb2.Response(interface="withdraw", result="success")
        else:
            return banks_pb2.Response(interface="withdraw", result="fail")

    def Propagate_Deposit(self, request):
        """Receives deposit propagation from other branches and updates balance."""
        self.balance += request.money
        return banks_pb2.Response(interface="propagate_deposit", result="success")

    def Propagate_Withdraw(self, request):
        """Receives withdrawal propagation from other branches and updates balance."""
        self.balance -= request.money
        return banks_pb2.Response(interface="propagate_withdraw", result="success")

    def Propagate_To_Branches(self, interface, money):
        """Propagates deposit or withdrawal actions to all other branches."""
        for stub in self.stubList:
            try:
                request = banks_pb2.Request(interface=interface, money=money)
                stub.MsgDelivery(request)
                time.sleep(0.1) 
            except grpc.RpcError as e:
                logger.error(f"Error propagating to branch: {e.details()}")

def serve(branch):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    banks_pb2_grpc.add_BankServiceServicer_to_server(branch, server)
    server.add_insecure_port(f'[::]:{50000 + branch.id}')
    server.start()
    logger.info(f"Branch server started at port {50000 + branch.id}")
    server.wait_for_termination()
