from sql_handler import SQLHandler
import re
import argparse
from tqdm import tqdm


def create_connection() -> SQLHandler:
    return SQLHandler()


BASE_CDN_URL = "https://cdn.pinapelz.com/VTuber%20Covers%20Archive/"

def update_m3u(file_path: str, output_path: str = "new_m3u.m3u"):
    server = create_connection()
    with open(file_path, "r") as f:
        output_file = open(output_path, "w")
        lines = f.readlines()
        pattern = r'/([^/]+)$'

        for line in tqdm(lines, desc="Processing lines"):
            match = re.search(pattern, line)
            if match:
                video_id = match.group(1).split(".")[0]
                query = f"SELECT extension FROM patchwork_archive.files WHERE video_id = '{video_id}'"
                result = server.get_query_result(query)
                if result:
                    extension = result[0][0]
                    output_file.write(f"{BASE_CDN_URL}{video_id}.{extension}\n")

        output_file.close()

def continue_m3u(file_path: str, output_path: str = "new_m3u.m3u"):
    server = create_connection()
    with open(file_path, "r") as f:
        output_file = open(output_path, "w")
        lines = f.readlines()
        if not lines:
            print("Input m3u file is empty.")
            return
        last_line = lines[-1]
        pattern = r'/([^/]+)$'
        match = re.search(pattern, last_line)
        if not match:
            print("Could not parse video id from last line.")
            return
        last_line_video_id = match.group(1).split(".")[0]

        # Use get_query_result for SELECT queries (returns rows)
        result = server.get_query_result(f"SELECT id FROM patchwork_archive.songs WHERE video_id = '{last_line_video_id}'")
        if not result:
            print(f"No song found with video_id '{last_line_video_id}'. Nothing to continue from.")
            return
        most_recent_id = result[0][0]

        unprocessed_songs = server.get_query_result(f"SELECT video_id FROM patchwork_archive.songs WHERE id > {most_recent_id}")
        if not unprocessed_songs:
            print("No new songs to process.")
            return

        for song in tqdm(unprocessed_songs, desc="Processing songs"):
            video_id = song[0]
            query = f"SELECT extension FROM patchwork_archive.files WHERE video_id = '{video_id}'"
            result = server.get_query_result(query)
            if result:
                extension = result[0][0]
                output_file.write(f"{BASE_CDN_URL}{video_id}.{extension}\n")

        output_file.close()

def merge_into_original_m3u(source_file: str, dest_file: str):
    prompt = input("Would you like to overwrite the original m3u file? (y/n) ")
    if prompt.lower() == "n":
        print("Got it. The output file is new_m3u.m3u")
        return
    with open(source_file, "r") as f:
        source_lines = f.readlines()
    with open(dest_file, "a") as f:
        for line in source_lines:
            f.write(line)
    import os
    os.remove(source_file)



def main():
    parser = argparse.ArgumentParser(prog="patchwork-radio-m3u-generator", description="Generate m3u playlist for Patchwork Radio")
    parser.add_argument("mode", type=str, help="Mode of operation: update_m3u, continue_m3u")
    parser.add_argument("file", type=str, help="Path to existing m3u file")
    parser.add_argument("--output", type=str, help="Output path for new m3u file")
    args = parser.parse_args()
    match args.mode:
        case "update_m3u":
            print("Running checks on m3u file to ensure extensions are correct")
            update_m3u(args.file)
            merge_into_original_m3u("new_m3u.m3u", args.file)
        case "continue_m3u":
            print("Continuing m3u file")
            continue_m3u(args.file)
            merge_into_original_m3u("new_m3u.m3u", args.file)
        case _:
            print("Invalid mode")
            exit(1)


if __name__ == "__main__":
    main()
