import argparse
import logging
import threading
import sys
from datetime import datetime
from time import time
import re
import ffmpeg


def record_video(input_stream_name: str, audio_stream_name: str | None, output_file_name: str):
    """
    Records video from the input stream and saves it to the output file.

    :param input_stream_name: Address of the input stream
    :param audio_stream_name: Address of the audio stream
    :param output_file_name: Name of the output file
    """
    in_stream = ffmpeg.input(input_stream_name)
    if audio_stream_name:
        audio_stream = ffmpeg.input(audio_stream_name)
        stream = ffmpeg.output(in_stream,
                               audio_stream,
                               output_file_name, vcodec="copy", acodec="aac")
    else:
        stream = ffmpeg.output(in_stream,
                               output_file_name,
                               vcodec="copy", acodec="aac")
    print(f"Recording of {input_stream_name} started")
    ffmpeg.run(stream, quiet=True, overwrite_output=True)


def get_stream_delay(stream_name: str) -> float:
    """
    Gets the "start_time" parameter of the video stream. Currently not used.

    :param stream_name: Name of the stream
    :return: "start_time" parameter of the video stream
    """
    probe = ffmpeg.probe(stream_name)
    video_stream = next(
        (stream for stream in probe["streams"] if stream["codec_type"] == "video"))
    if not video_stream:
        raise ValueError("No video stream found")
    return float(video_stream["start_time"])


def make_filename(stream_name: str) -> str:
    """
    Creates a filename for the output file based on the input stream name and current time.

    :param stream_name: Address of the input stream
    :return: Filename for the output file
    """
    ip = re.search(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", stream_name).group(0)
    now = datetime.now()
    return f"{ip}_{now.strftime('%Y-%m-%d_%H-%M-%S')}.mp4"


def create_thread(input_stream_name: str, audio_stream_name: str | None) -> threading.Thread:
    """
    Creates a thread for recording video from the input stream.

    :param input_stream_name: Address of the input stream
    :param audio_stream_name: Address of the audio stream
    :return: Thread for recording video from the input stream
    """
    thread = threading.Thread(target=record_video, args=(
        input_stream_name, audio_stream_name, get_filename(input_stream_name)))
    thread.daemon = True
    thread.start()
    return thread


def create_queue(input_stream_names: list[str], delays: list[float] | None) -> list[tuple[str, float]]:
    """
    Creates a queue of video streams.

    :param input_stream_names: Addresses of the input video streams
    :param delays: Streams offsets in seconds for synchronization
    :return: Queue of video streams
    """
    logging.debug(f"Creating queue for {input_stream_names}...")
    queue: list[tuple[str, float]] = []
    if delays:
        for input_stream_name, delay in zip(input_stream_names, delays):
            queue.append((input_stream_name, delay))
    else:
        for input_stream_name in input_stream_names:
            delay = get_stream_delay(input_stream_name)
            logging.debug(f"Delay for {input_stream_name} is {delay}s")
            queue.append((input_stream_name, delay))
    queue.sort(key=lambda x: x[1])
    # find minimal delay and subtract it from all delays
    min_delay = min(queue, key=lambda x: x[1])[1]
    for i in range(len(queue)):
        queue[i] = (queue[i][0], queue[i][1] - min_delay)
    for stream_name, delay in queue:
        logging.debug(f"{stream_name} will start with delay of {delay}s")
    return queue


def start_queue(queue: list[tuple[str, float]], audio_stream_name: str | None) -> list[threading.Thread]:
    """
    Starts recording of video streams from the queue.

    :param queue: Queue of video streams
    :param audio_stream_name: Address of the audio stream
    :return: List of threads for recording video streams
    """
    threads = []
    begin = time()
    while queue:
        stream_name, delay = queue.pop(0)
        while time() - begin < delay:
            pass
        thread = create_thread(stream_name, audio_stream_name)
        threads.append(thread)
    return threads


def main(input_stream_names: list[str], audio_stream_name: str | None = None, delays: list[float] | None = None):
    """
    Starts recording of several video streams and mixing audio from microphone to them.

    :param input_stream_names: Addresses of the input video streams
    :param audio_stream_name: Address of the input audio stream
    """
    if delays:
        if len(input_stream_names) != len(delays):
            raise ValueError("Number of delays must be equal to number of input streams")
    queue: list[tuple[str, float]] = create_queue(input_stream_names, delays)
    logging.debug("Starting recording...")
    threads : list[threading.Thread] = start_queue(queue, audio_stream_name)
    try:
        while True:
            for thread in threads:
                if not thread.is_alive():
                    logging.warning(f"Restarting {thread}, because it is dead")
                    ind = threads.index(thread)
                    thread = create_thread(
                        input_stream_names[ind], audio_stream_name)
                    threads[ind] = thread
    except KeyboardInterrupt:
        logging.info("Got keyboard interrupt, exiting...")
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="Tool for synchronous recording of several "
                                     "video streams and mixing audio from microphone to them.")
    parser.add_argument("-i", "--input", type=str, nargs="+", required=True,
                        help="Input video streams")
    parser.add_argument("-a", "--audio", type=str,
                        required=False, help="Input audio stream")
    parser.add_argument("-d", "--delays", type=float, nargs="+", required=False,
                        help="Streams offsets in seconds for synchronization; order must be the same as in input streams")
    args = parser.parse_args()
    main(args.input, args.audio, args.delays)
    # main(["rtsp://172.18.191.105:554/Streaming/Channels/1", "rtsp://172.18.191.106:554/Streaming/Channels/1", "rtsp://172.18.191.104:554/user=admin_password=BhcGS01Q_channel=1_stream=0.sdp?real_stream"], "rtsp://172.18.191.101/0")
    # main(["rtsp://172.18.191.105:554/Streaming/Channels/1", "rtsp://172.18.191.106:554/Streaming/Channels/1", "rtsp://172.18.191.104:554/user=admin_password=BhcGS01Q_channel=1_stream=0.sdp?real_stream"])


