import socket

def has_network(host="8.8.8.8", port=53, timeout=1):
    # Return True if network connection can be established
    try:
        socket.create_connection((host, port), timeout=timeout)
        return True
    except OSError:
        return False