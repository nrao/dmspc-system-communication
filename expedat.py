import subprocess
import select
import os
from dotenv import load_dotenv
import random

load_dotenv()


def expedat_send(mvd_filepath, method):
    """
    Send one raw-data file from one directory/machine to another using expedat.
    Supports both Transfer and Stream methods

    movedat (mvd): sender
    servedat (svd): receiver

    """

    svd_password = os.environ["SVD_PASSWORD"]
    svd_ip = os.environ["SVD_IP"]
    svd_user = os.environ["SVD_USER"]
    recipient_directory = os.environ["RECIPIENT_DIR"]

    #location where movedat is saved on my computer:
    mvd_location = os.environ["MVD_LOC"]

    if method == "transfer":

        master_fd, receiver_fd = os.openpty()

        # transfer method requires a filepath to retrieve the completed file
        terminal_command = [
                "./movedat",
                mvd_filepath,
                f"{svd_user}:{svd_password}@{svd_ip}:{recipient_directory}",
            ]
        
        process = subprocess.Popen(
            terminal_command,
            stdin=receiver_fd,
            stdout=receiver_fd,
            stderr=receiver_fd,
            close_fds=True,
            cwd=mvd_location
        )

        os.close(receiver_fd)

        try:
            while process.poll() is None:
                readable, _, _ = select.select(
                    [master_fd],
                    [],
                    [],
                    0.5,
                )
    
                if not readable:
                    continue
    
                try:
    
                    terminal_output = os.read(master_fd, 4096).decode(
                        "utf-8",
                        errors="replace",
                    )
    
                except OSError:
                    break
    
                # Print the actual etc output to Docker logs.
                print(terminal_output, end="", flush=True)
    
        finally:
            os.close(master_fd)

    elif method == "stream":

        # stream method uses "-" in place of filepath, because the file does not exist anywhere yet
        terminal_command = [
                    "./movedat",
                    "-",
                    f"{svd_user}:{svd_password}@{svd_ip}:{recipient_directory}/generated_file.txt",
                ]

        # Standard Input IN (stdin) and Standard Input OUT (stdout):
        # creates a pipe connecting the Python process to the movedat process
        # allows the Python file generation to inform movedat, and vice versa
        process = subprocess.Popen(
            terminal_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=mvd_location,
        )
        try:
            file_size_bytes = 10 * 1024 * 1024
            num_buffers = 100

            # X amount of buffers divides the file into X pieces to be
            # sent to movedat as each piece is written
            buffer_size = file_size_bytes // num_buffers
            remainder = file_size_bytes % num_buffers

            for index in range(num_buffers):
                size = buffer_size + (
                    1 if index < remainder else 0
                )

                # the randomly generated data:
                buffer = random.randbytes(size)

                process.stdin.write(buffer)
                process.stdin.flush()

            # No more data is coming.
            process.stdin.close()

            # Read movedat output after sending the data.
            for output in process.stdout:
                print(
                    output.decode(
                        "utf-8",
                        errors="replace",
                    ),
                    end="",
                    flush=True,
                )

        finally:
            if process.stdin and not process.stdin.closed:
                process.stdin.close()

            if process.stdout:
                process.stdout.close()

    else:
        raise ValueError(
            "Invalid method: expected "
            "'transfer' or 'stream'."
        )

    return_code = process.wait()

    if return_code != 0:
        raise subprocess.CalledProcessError(
            return_code,
            process.args,
        )

mvd_filepath = os.environ["MVD_FILEPATH"]
method = "stream"
expedat_send(mvd_filepath, method)