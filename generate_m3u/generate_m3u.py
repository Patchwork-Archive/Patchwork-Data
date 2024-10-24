import mysql.connector
from mysql.connector import Error, errorcode
from sql_handler import SQLHandler
import configparser
import re
import argparse
from tqdm import tqdm


def read_config(file_path: str):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

CONFIG = read_config("config.ini")


def create_connection() -> SQLHandler:
    hostname = CONFIG.get("database", "host")
    user = CONFIG.get("database", "user")
    password = CONFIG.get("database", "password")
    database = CONFIG.get("database", "database")
    return SQLHandler(hostname, user, password, database)


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
                query = f"SELECT extension FROM files WHERE video_id = '{video_id}'"
                result = server.get_query_result(query)
                if result:
                    extension = result[0][0]
                    output_file.write(f"{BASE_CDN_URL}{video_id}.{extension}\n")

        output_file.close()

def continue_m3u(file_path: str, output_path: str = "new_m3u.m3u"):
    server = create_connection()
    with open(file_path, "r") as f:
        output_file = open(output_path, "w")
        last_line = f.readlines()[-1]
        pattern = r'/([^/]+)$'
        last_line_video_id = re.search(pattern, last_line).group(1).split(".")[0]
        most_recent_id = server.execute_query(f"SELECT id FROM songs WHERE video_id = '{last_line_video_id}'")[0][0]

        unprocessed_songs = server.execute_query(f"SELECT video_id FROM songs WHERE id > {most_recent_id}")
        for song in tqdm(unprocessed_songs, desc="Processing songs"):
            video_id = song[0]
            query = f"SELECT extension FROM files WHERE video_id = '{video_id}'"
            result = server.get_query_result(query)
            if result:
                extension = result[0][0]
                output_file.write(f"{BASE_CDN_URL}{video_id}.{extension}\n")

def merge_into_original_m3u(source_file: str, dest_file: str):
    prompt = input("Would you like to overwrrite the original m3u file? (y/n)")
    if prompt == "n":
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
