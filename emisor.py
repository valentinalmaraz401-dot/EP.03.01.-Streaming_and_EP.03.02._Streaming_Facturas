import socket
import time
import random

def enviar_palabra(palabra, host='localhost', puerto=8888):
    cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente_socket.connect((host, puerto))
        palabra = palabra + '\n'
        cliente_socket.sendall(palabra.encode())
        intervalo = random.uniform(0.5, 1.5)
        time.sleep(intervalo)
    finally:
        cliente_socket.close()

if __name__ == "__main__":
    palabras = ["hola", "mundo", "python", "netcat", "socket"] * 2
    for palabra in palabras:
        print(f"Enviando: {palabra}")
        enviar_palabra(palabra)
