#!/usr/bin/env python3
import argparse
import asyncio
import asyncpipe
import subprocess as sp
import zipfile
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor

import eyed3
from eyed3.id3 import ID3_V2_4

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('bcconvert')
logger.setLevel(logging.DEBUG)


HAPPY_ZIP_PAT = re.compile(r'\A([\w\s\d]+) - ([\w\s\d]+)\Z')
CHECK_ZIP_PAT = re.compile(r'\A(.+) - (.+)\Z')


def check_commands():
    missing = []
    for cmd in ['flac', 'lame', 'metaflac']:
        try:
            sp.check_call([cmd, '--help'], stdout=sp.PIPE, stderr=sp.PIPE)
        except EnvironmentError as exc:
            missing.append(cmd)
    if missing:
        raise RuntimeError('missing commands: {}'.format(missing))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--unpack-dir',
                        default=os.path.expanduser('~/Music/extracted'))
    parser.add_argument('--no-descend', action='store_false', dest='descend')
    parser.add_argument('paths', nargs='+', metavar='path')
    return parser.parse_args()


def recurse(path):
    for root, _, files in os.walk(path):
        for filename in files:
            yield os.path.join(root, filename)


def shallow(path):
    for filename in os.listdir(path):
        fullpath = os.path.join(path, filename)
        if os.path.isfile(fullpath):
            yield fullpath


def get_all_matching(path, search_ext, descend=True):
    search_ext = search_ext.lower()
    if path.lower().endswith(search_ext):
        yield path
        return

    if descend:
        iterator = recurse(path)
    else:
        iterator = shallow(path)
    yield from (p for p in iterator if p.lower().endswith(search_ext))


def generate_dest_dir_name(path):
    filename, ext = os.path.splitext(path)
    basename = os.path.basename(filename)
    assert ext.lower() == '.zip'
    match = HAPPY_ZIP_PAT.match(basename)
    if match:
        return os.path.join(*match.groups())
    match = CHECK_ZIP_PAT.match(basename)
    if match:
        logger.warning('Did not match the happy zip pattern for {}, check names'.format(path))
        return os.path.join(*match.groups())
    logger.error('Zip file {} does not match the expected pattern!'.format(path))
    return None



def unzip_serial(path, dest_root):
    dest_dir_name = generate_dest_dir_name(path)
    if not dest_dir_name:
        return
    dest = os.path.join(dest_root, dest_dir_name)
    if os.path.exists(dest):
        logger.warning('Destination path {} already exists, skipping unzip'
                       .format(dest))
        return
    os.makedirs(dest)
    with zipfile.ZipFile(path, 'r') as zfp:
        zfp.extractall(dest)
    logger.info('Unzipped to {}'.format(dest))
    return dest


def _setter(attr, f=None):
    if f is None:
        f = lambda x: x
    def func(value):
        def innermost(tag):
            setattr(tag, attr, f(value))
        return innermost
    return func


TAG_MAP = {
    b'TITLE': _setter('title'),
    b'ARTIST': _setter('artist'),
    b'ALBUM': _setter('album'),
    b'TRACKNUMBER': _setter('track_num', lambda x: (int(x), None)),
    b'DATE': _setter('year'),
    # albumartist is not well-standardized anywhere. BC at least is kind enough
    # to consistently use ALBUMARTIST, I think.
    b'ALBUMARTIST': _setter('album_artist'),
    # I throw away lyrics/comment because I don't care about them
}


def metaflac_stdout_to_setters(output):
    """note to self: this only gets the first line of multiline tags.
    output looks like this (imagine the 'M' in METADATA is fully unindented):

        METADATA block #2
          type: 4 (VORBIS_COMMENT)
          is last: false
          length: 1640
          vendor string: reference libFLAC 1.3.0 20130526
          comments: 8
            comment[0]: TITLE=Track title
            comment[1]: ARTIST=Artist name
            comment[2]: DATE=2018
            comment[3]: COMMENT=some stupid comment
            comment[4]: ALBUM=Album name
            comment[5]: TRACKNUMBER=70
            comment[6]: ALBUMARTIST=Album artist
            comment[7]: UNSYNCEDLYRICS=The first line of lyrics
        and then the second line which is silly
        blah blah blah

    so multiline is slightly harder, and I don't care about multline
    """
    for line in output.split(b'\n'):
        match = re.match(
            br'\s+comment\[\d+\]: ([A-Za-z 0-9_]+)=(.*)',
            line
        )
        if match:
            key, value = match.groups()
            try:
                attrfunc = TAG_MAP[key]
            except KeyError:
                continue
            yield attrfunc(value.decode('utf-8'))


def metaflac_cmd_args(flac_path):
    return [
        'metaflac', '--list', '--no-utf8-convert',
        '--block-type=VORBIS_COMMENT', flac_path
    ]

def set_tags(path, setters):
    mp3 = eyed3.load(path)
    for setter in setters:
        setter(mp3.tag)
    mp3.tag.save(version=ID3_V2_4)


async def unzip(loop, path, dest_root):
    """TODO: maybe make this parallel"""
    return unzip_serial(path, dest_root)


async def get_tag_setters(loop, flac_path):
    cmd = metaflac_cmd_args(flac_path)
    with open(os.devnull, 'w') as devnull:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=devnull,
            loop=loop,
        )
        stdout, _ = await proc.communicate()
    return list(metaflac_stdout_to_setters(stdout))



async def pipe_cmds(loop, cmd_args_1, cmd_args_2):
    """Pipe cmd 1 to cmd 2 by wiring up stdin/stdout.
    This would be pretty easy to extend to N processes, I just don't need that.
    """
    pipe = asyncpipe.PipeBuilder(loop=loop)
    pipe = pipe.chain(*cmd_args_1).chain(*cmd_args_2)
    result = await pipe.call_async()
    return result[-1].returncode


async def convert_file_only(loop, flac_path, mp3_path):
    setters = await get_tag_setters(loop, flac_path)

    flac_args = ['flac', '--silent', '--stdout', '--decode', flac_path]
    
    lame_args = ['lame', '-V0', '--add-id3v2', '--pad-id3v2']
    lame_args.extend(['-', mp3_path])

    returncode = await pipe_cmds(loop, flac_args, lame_args)

    if returncode != 0:
        try:
            os.remove(mp3_path)
        except:
            pass
        raise RuntimeError('lame failed!')


async def convert_flac(loop, flac_path):
    mp3_path = os.path.splitext(flac_path)[0] + '.mp3'
    if os.path.exists(mp3_path):
        logger.warning('file already exists at {}, skipping'.format(mp3_path))
        return

    tag_setters = get_tag_setters(loop, flac_path)
    conversion = convert_file_only(loop, flac_path, mp3_path)

    try:
        setters, _ = await asyncio.gather(tag_setters, conversion,
                                                 loop=loop)
    except RuntimeError:
        return

    set_tags(mp3_path, setters)
    logger.debug('wrote tag for {}, removing original'
                 .format(mp3_path))
    os.remove(flac_path)
    return mp3_path


async def main_loop(loop, dest_root, paths, descend):
    all_converted_files = []
    for path in paths:
        for zippath in get_all_matching(path, '.zip', descend=descend):
            dirname = await unzip(loop, zippath, dest_root)
            if dirname is None:
                continue
            for flac_path in get_all_matching(dirname, '.flac'):
                all_converted_files.append(convert_flac(loop, flac_path))
    await asyncio.gather(*all_converted_files)


def main():
    parsed = parse_args()
    check_commands()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        main_loop(loop, parsed.unpack_dir, parsed.paths, parsed.descend)
    )
    logger.info('Done!')


if __name__ == '__main__':
    main()
