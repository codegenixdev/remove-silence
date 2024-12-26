import subprocess
import json
import os
import re
import tempfile
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging
import shutil
import sys
from datetime import datetime

# Set up logging
log_filename = f"silence_removal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class VideoProcessingError(Exception):
    """Custom exception for video processing errors"""
    pass

def check_system_requirements():
    """Check if all required system components are available"""
    try:
        # Check FFmpeg
        ffmpeg_version = subprocess.run(
            ['ffmpeg', '-version'], 
            capture_output=True, 
            text=True
        ).stdout.split('\n')[0]
        logger.info(f"FFmpeg version: {ffmpeg_version}")

        # Check FFprobe
        ffprobe_version = subprocess.run(
            ['ffprobe', '-version'], 
            capture_output=True, 
            text=True
        ).stdout.split('\n')[0]
        logger.info(f"FFprobe version: {ffprobe_version}")

        # Check available disk space
        total, used, free = shutil.disk_usage(os.getcwd())
        free_gb = free // (2**30)
        logger.info(f"Available disk space: {free_gb} GB")
        
        if free_gb < 10:
            logger.warning("Low disk space! Recommended: at least 10 GB free")
            
        return True
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        logger.error(f"System requirements check failed: {str(e)}")
        return False

def check_input_files():
    """Check if input files exist and are accessible"""
    current_dir = os.path.abspath(os.getcwd())
    mkv_files = glob.glob(os.path.join(current_dir, "????-??-?? ??-??-??.mkv"))
    
    if not mkv_files:
        logger.error("No MKV files found in the current directory")
        logger.info("Expected format: 'YYYY-MM-DD HH-MM-SS.mkv'")
        logger.info(f"Current directory: {current_dir}")
        return False
        
    all_accessible = True
    total_size = 0
    
    for file in mkv_files:
        if not os.path.isfile(file):
            logger.error(f"File not found: {file}")
            all_accessible = False
        elif not os.access(file, os.R_OK):
            logger.error(f"File not readable: {file}")
            all_accessible = False
        else:
            size_mb = os.path.getsize(file) / (1024 * 1024)
            total_size += size_mb
            logger.info(f"Found file: {os.path.basename(file)} ({size_mb:.2f} MB)")
            
    logger.info(f"Total size of input files: {total_size:.2f} MB")
    return all_accessible

def merge_mkv_files(output_file):
    """Merge all MKV files in the current directory with the specified date format"""
    current_dir = os.path.abspath(os.getcwd())
    mkv_files = sorted(glob.glob(os.path.join(current_dir, "????-??-?? ??-??-??.mkv")))
    
    if not mkv_files:
        logger.warning("No MKV files found with the specified format.")
        return False

    logger.info(f"Found {len(mkv_files)} MKV files to merge")

    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        temp_list = f.name
        for file in mkv_files:
            abs_path = os.path.abspath(file)
            escaped_path = abs_path.replace('\\', '\\\\')
            f.write(f"file '{escaped_path}'\n")

    try:
        cmd = [
            "ffmpeg",
            "-f", "concat",
            "-safe", "0",
            "-i", temp_list,
            "-c", "copy",
            "-y",
            output_file
        ]
        
        logger.info("Merging files...")
        result = subprocess.run(
            cmd, 
            check=True, 
            capture_output=True, 
            text=True
        )
        
        if os.path.exists(output_file):
            file_size = os.path.getsize(output_file) / (1024 * 1024)
            logger.info(f"Successfully created merged file: {output_file} ({file_size:.2f} MB)")
            return True
        else:
            logger.error("Merge completed but output file not found")
            return False
            
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg error: {e}")
        logger.error(f"FFmpeg stderr: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False
    finally:
        try:
            if os.path.exists(temp_list):
                os.remove(temp_list)
        except Exception as e:
            logger.warning(f"Could not remove temporary file: {str(e)}")

def detect_silence(input_file, noise_threshold="-40dB", duration=0.5):
    """Detect silent segments in the video"""
    cmd = [
        "ffmpeg",
        "-i", input_file,
        "-af", f"silencedetect=noise={noise_threshold}:d={duration}",
        "-f", "null",
        "-"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        starts = re.findall(r'silence_start: ([\d\.]+)', result.stderr)
        ends = re.findall(r'silence_end: ([\d\.]+)', result.stderr)
        
        silence_periods = list(zip([float(x) for x in starts], [float(x) for x in ends]))
        logger.info(f"Detected {len(silence_periods)} silence periods")
        
        return silence_periods
    except subprocess.CalledProcessError as e:
        logger.error(f"Error detecting silence: {e}")
        raise VideoProcessingError("Failed to detect silence periods")

def get_video_duration(input_file):
    """Get the duration of a video file"""
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        input_file
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return float(json.loads(result.stdout)['format']['duration'])
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error getting video duration: {e}")
        raise VideoProcessingError("Failed to get video duration")

def create_cut_list(silences, total_duration, min_segment_duration=0.2, padding=0.1):
    """Create a list of segments to keep"""
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

def process_segment(input_file, temp_dir, segment_info):
    """Process a single video segment"""
    index, (start, end) = segment_info
    temp_output = os.path.join(temp_dir, f"segment_{index:04d}.mp4")
    duration = end - start
    
    cmd = [
        "ffmpeg",
        "-ss", str(start),
        "-i", input_file,
        "-t", str(duration),
        "-c:v", "libx264",
        "-preset", "faster",
        "-crf", "18",
        "-c:a", "aac",
        "-b:a", "192k",
        "-avoid_negative_ts", "1",
        "-y",
        temp_output
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return temp_output
    except subprocess.CalledProcessError as e:
        logger.error(f"Error processing segment {index}: {e}")
        return None

def process_chunks_parallel(input_file, output_file, cut_list, max_workers=4):
    """Process video chunks in parallel"""
    with tempfile.TemporaryDirectory() as temp_dir:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, segment in enumerate(cut_list):
                futures.append(
                    executor.submit(process_segment, input_file, temp_dir, (i, segment))
                )
            
            processed_segments = []
            with tqdm(total=len(futures), desc="Processing segments") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        processed_segments.append(result)
                    pbar.update(1)
        
        processed_segments.sort()
        
        concat_file = os.path.join(temp_dir, "concat_list.txt")
        with open(concat_file, 'w') as f:
            for segment in processed_segments:
                f.write(f"file '{segment}'\n")
        
        cmd = [
            "ffmpeg",
            "-f", "concat",
            "-safe", "0",
            "-i", concat_file,
            "-c", "copy",
            "-y",
            output_file
        ]
        
        subprocess.run(cmd, check=True)

def remove_silence(
    input_file, 
    output_file, 
    noise_threshold="-40dB", 
    min_silence_duration=0.5, 
    min_segment_duration=0.2,
    padding_duration=0.1,
    max_workers=4
):
    """Main function to remove silence from video"""
    try:
        logger.info("Detecting silence...")
        silences = detect_silence(input_file, noise_threshold, min_silence_duration)
        
        if not silences:
            logger.warning("No silences detected. Check noise threshold.")
            return
            
        total_duration = get_video_duration(input_file)
        logger.info(f"Video duration: {total_duration:.2f} seconds")
        
        logger.info("Creating cut list...")
        cut_list = create_cut_list(
            silences, 
            total_duration, 
            min_segment_duration,
            padding_duration
        )
        
        if not cut_list:
            logger.warning("No segments to cut!")
            return
            
        logger.info(f"Created {len(cut_list)} segments to keep")
        
        logger.info("Processing video segments in parallel...")
        process_chunks_parallel(input_file, output_file, cut_list, max_workers)
        
        new_duration = get_video_duration(output_file)
        reduction = (1 - new_duration / total_duration) * 100
        
        logger.info("\nProcessing complete!")
        logger.info(f"Original duration: {total_duration:.2f} seconds")
        logger.info(f"New duration: {new_duration:.2f} seconds")
        logger.info(f"Reduced by: {reduction:.1f}%")
        logger.info(f"Output saved as: {output_file}")
        
    except Exception as e:
        logger.error(f"Error during silence removal: {str(e)}")
        raise

def cleanup_temp_files():
    """Clean up any temporary files"""
    patterns = ['*.txt', '*.tmp']
    for pattern in patterns:
        for file in glob.glob(pattern):
            try:
                os.remove(file)
            except Exception as e:
                logger.warning(f"Could not remove temporary file {file}: {str(e)}")

def main():
    try:
        logger.info("Starting video processing...")
        
        if not check_system_requirements():
            logger.error("System requirements not met. Exiting.")
            return

        if not check_input_files():
            logger.error("Input file check failed. Exiting.")
            return

        merged_file = "merged_input.mp4"
        output_file = "output_no_silence.mp4"
        
        logger.info("Starting merge process...")
        if not merge_mkv_files(merged_file):
            logger.error("Failed to merge MKV files. Exiting.")
            return

        try:
            max_workers = min(os.cpu_count() or 4, 8)
            logger.info(f"Using {max_workers} worker threads")
            
            remove_silence(
                merged_file,
                output_file,
                noise_threshold="-38dB",
                min_silence_duration=0.15,
                min_segment_duration=0.1,
                padding_duration=0.05,
                max_workers=max_workers
            )
        finally:
            if os.path.exists(merged_file):
                try:
                    os.remove(merged_file)
                    logger.info("Cleaned up merged input file")
                except Exception as e:
                    logger.warning(f"Could not remove merged file: {str(e)}")
    
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    finally:
        cleanup_temp_files()
        logger.info("Processing completed")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        cleanup_temp_files()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        cleanup_temp_files()
        sys.exit(1)