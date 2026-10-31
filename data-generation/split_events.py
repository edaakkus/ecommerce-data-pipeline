from pathlib import Path

INPUT_FILE = Path("events_5m.jsonl")
MAX_SIZE = 90 * 1024 * 1024  # 90 MB

part = 1
current_size = 0
output_file = None

with INPUT_FILE.open("r", encoding="utf-8") as source:

    for line in source:

        line_size = len(line.encode("utf-8"))

        if output_file is None or current_size + line_size > MAX_SIZE:

            if output_file is not None:
                output_file.close()

            filename = f"events_part_{part:02d}.jsonl"
            output_file = open(
                filename,
                "w",
                encoding="utf-8",
                newline=""
            )

            print(f"Oluşturuluyor: {filename}")

            current_size = 0
            part += 1

        output_file.write(line)
        current_size += line_size

if output_file is not None:
    output_file.close()

print("PARÇALAMA TAMAMLANDI.")