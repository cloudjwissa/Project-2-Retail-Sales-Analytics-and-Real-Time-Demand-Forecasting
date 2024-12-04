import time
import random
import socket
from datetime import datetime

# Generate sample Product IDs
products = [f"P{i:03}" for i in range(1, 301)]  # P001, P002, ..., P300

# Function to generate random stock prices
def generate_transaction():
    product_id = random.choice(products)
    quantity = random.randint(1, 80)  # Random quantity between 1 and 80
    price = round(random.uniform(1, 1000), 2)  # Random price between $1 and $1000
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{timestamp},{product_id},{quantity},{price}"

def start_streaming(port1, port2):
    # Create socket servers for each port
    server_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket1.bind(('localhost', port1))
    server_socket1.listen(1)

    server_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket2.bind(('localhost', port2))
    server_socket2.listen(1)

    print(f"Waiting for connections on ports {port1} and {port2}... (Press Ctrl+C to stop)")

    connection1, addr1 = server_socket1.accept()
    connection2, addr2 = server_socket2.accept()

    print("Connections established. Streaming data...")

    try:
        while True:
            #start the data stream
            transaction_data = generate_transaction()
            #send the stream to each port
            connection1.send((transaction_data + "\n").encode('utf-8'))
            connection2.send((transaction_data + "\n").encode('utf-8'))
            print(f"Sent: {transaction_data}")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")

    finally:
        connection1.close()
        connection2.close()
        server_socket1.close()
        server_socket2.close()

if __name__ == "__main__":
    start_streaming(9999, 9998)
