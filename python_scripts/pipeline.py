import argparse
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--gen', action='store_true')

    args = parser.parse_args()

    # If --gen FLAG IS PROVIDED, RUN GENERATE.PY
    if args.gen:
        print("Generating data...")
        print("#" * 70 + "\n")

        subprocess.run(["python", "generate.py", "--n1", "1", "--n2", "9"], check=True)


    # RUN THE PROCESSING SCRIPTS
    scripts = ["simple_process.py", "pyspark_process.py"]

    for script in scripts:
        print(f"\nRunning {script}...")
        print("#" * 70 + "\n")

        subprocess.run(["python", script], check=True)


if __name__ == "__main__":
    main()