import subprocess


def get_windows_host_ip():
    try:
        # Chạy lệnh shell để lấy dòng default route
        output = subprocess.check_output(['ip', 'route'], text=True)
        for line in output.splitlines():
            if line.startswith('default via'):
                parts = line.split()
                return parts[2]  # IP nằm ở vị trí thứ 3
    except Exception as e:
        print(f"Lỗi: {e}")
        return None


# Stop streaming when user presses 'q'

def wait_for_exit_spark_stream(spark_stream=None):
    while True:
        key = input("Press 'q' to stop streaming:\n")
        if key.strip().lower() == 'q':
            print("Stopping streaming...")
            spark_stream.stop()
            break