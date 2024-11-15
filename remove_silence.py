import subprocess
import json
import os
import re
import tempfile
import glob 

def merge_mkv_files(output_file):
    """Merge all MKV files in the current directory with the specified date format"""
    mkv_files = sorted(glob.glob("????-??-?? ??-??-??.mkv"))
    
    if not mkv_files:
        print("No MKV files found with the specified format.")
        return False

    with open("temp_file_list.txt", "w") as f:
        for file in mkv_files:
            # Escape single quotes in filenames
            escaped_file = file.replace("'", "'\\''")
            f.write(f"file '{escaped_file}'\n")

    cmd = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", "temp_file_list.txt",
        "-c", "copy",
        "-y",
        output_file
    ]
    
    subprocess.run(cmd, check=True)
    os.remove("temp_file_list.txt")
    return True

def detect_silence(input_file, noise_threshold="-40dB", duration=0.5):
    cmd = [
        "ffmpeg",
        "-i", input_file,
        "-af", f"silencedetect=noise={noise_threshold}:d={duration}",
        "-f", "null",
        "-"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    start_pattern = r'silence_start: ([\d\.]+)'
    end_pattern = r'silence_end: ([\d\.]+)'
    
    starts = re.findall(start_pattern, result.stderr)
    ends = re.findall(end_pattern, result.stderr)
    
    silence_starts = [float(x) for x in starts]
    silence_ends = [float(x) for x in ends]
    
    return list(zip(silence_starts, silence_ends))

def get_video_duration(input_file):
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        input_file
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    
    return float(data['format']['duration'])

def create_cut_list(silences, total_duration, min_segment_duration=0.2, padding=0.1):
    cut_list = []
    last_end = 0
    
    for start, end in silences:
        segment_start = max(0, last_end - padding)
        segment_end = max(segment_start, start + padding)
        
        if segment_end - segment_start >= min_segment_duration:
            cut_list.append((segment_start, segment_end))
        
        last_end = end
    
    if total_duration - last_end >= min_segment_duration:
        cut_list.append((max(last_end - padding, 0), total_duration))
    
    return cut_list

def process_chunk(input_file, output_file, cut_list, chunk_start, chunk_end):
    filter_complex = ""
    for i, (start, end) in enumerate(cut_list[chunk_start:chunk_end]):
        filter_complex += f"[0:v]trim={start}:{end},setpts=PTS-STARTPTS[v{i}];"
        filter_complex += f"[0:a]atrim={start}:{end},asetpts=PTS-STARTPTS[a{i}];"
    
    filter_complex += "".join(f"[v{i}][a{i}]" for i in range(chunk_end - chunk_start))
    filter_complex += f"concat=n={chunk_end - chunk_start}:v=1:a=1[outv][outa]"
    
    cmd = [
        "ffmpeg",
        "-i", input_file,
        "-filter_complex", filter_complex,
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "18",
        # Remove or adjust the frame rate to match input
        # "-r", "24",  # Removed
        "-c:a", "aac",
        "-b:a", "192k",
        "-vsync", "1",         # Added
        "-async", "1",         # Added
        "-y",
        output_file
    ]
    
    subprocess.run(cmd, check=True)
    print(f"Processed chunk: {output_file}")

def remove_silence(
    input_file, 
    output_file, 
    noise_threshold="-40dB", 
    min_silence_duration=0.5, 
    min_segment_duration=0.2,
    padding_duration=0.1
):
    try:
        print("Detecting silence...")
        silences = detect_silence(input_file, noise_threshold, min_silence_duration)
        
        if not silences:
            print("No silences detected. Check noise threshold.")
            return
            
        print(f"Found {len(silences)} silent segments")
        
        total_duration = get_video_duration(input_file)
        print(f"Video duration: {total_duration:.2f} seconds")
        
        print("Creating cut list...")
        cut_list = create_cut_list(
            silences, 
            total_duration, 
            min_segment_duration,
            padding_duration
        )
        
        if not cut_list:
            print("No segments to cut!")
            return
            
        print(f"Created {len(cut_list)} segments to keep")
        
        print("Processing video in chunks...")
        chunk_size = 50 # Process 50 segments at a time
        temp_files = []
        
        with tempfile.TemporaryDirectory() as temp_dir:
            for i in range(0, len(cut_list), chunk_size):
                chunk_end = min(i + chunk_size, len(cut_list))
                temp_output = os.path.join(temp_dir, f"temp_output_{i}.mp4")
                process_chunk(input_file, temp_output, cut_list, i, chunk_end)
                temp_files.append(temp_output)
            
            print("Concatenating processed chunks...")
            concat_file = os.path.join(temp_dir, "concat_list.txt")
            with open(concat_file, 'w') as f:
                for temp_file in temp_files:
                    f.write(f"file '{temp_file}'\n")
            
            cmd = [
                "ffmpeg",
                "-f", "concat",
                "-safe", "0",
                "-i", concat_file,
                "-c:v", "libx264",
                "-preset", "medium",
                "-crf", "18",
                "-c:a", "aac",
                "-b:a", "192k",
                "-avoid_negative_ts", "1",  # Added to handle timestamps
                "-vsync", "1",              # Added
                "-async", "1",              # Added
                "-y",
                output_file
            ]
            subprocess.run(cmd, check=True)
        
        # Temporary directory and its contents are automatically cleaned up
        
        new_duration = get_video_duration(output_file)
        reduction = (1 - new_duration / total_duration) * 100
        
        print(f"\nProcessing complete!")
        print(f"Original duration: {total_duration:.2f} seconds")
        print(f"New duration: {new_duration:.2f} seconds")
        print(f"Reduced by: {reduction:.1f}%")
        print(f"Output saved as: {output_file}")
        
    except subprocess.CalledProcessError as e:
        print(f"FFmpeg error: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

def main():
    merged_file = "merged_input.mp4"
    output_file = "output_no_silence.mp4"
    
    print("Merging MKV files...")
    if not merge_mkv_files(merged_file):
        print("No files to process. Exiting.")
        return

    print(f"Merged file created: {merged_file}")
    
    remove_silence(
        merged_file,
        output_file,
        noise_threshold="-38dB",
        min_silence_duration=0.15,
        min_segment_duration=0.1,
        padding_duration=0.05
    )
    
    # Clean up the merged file
    os.remove(merged_file)

if __name__ == "__main__":
    main()