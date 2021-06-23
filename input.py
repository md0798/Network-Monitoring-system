import socket
import time
import csv

host = 'localhost'  # The server's hostname or IP address
port = 1234  # The port used by the server

input_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
input_socket.connect((host, port))

with open("input.csv", "r") as f:
    data = csv.reader(f, delimiter=' ')

    for row in data:
        row = str(row)
        row = row[2:-2]
        output = "#" + row
        print("sending: ", output)
        input_socket.send(output.encode())
        time.sleep(0.0005)