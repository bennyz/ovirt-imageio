# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import os
import tarfile

import pytest

from ovirt_imageio import client
from ovirt_imageio._internal import blkhash
from ovirt_imageio._internal import config
from ovirt_imageio._internal import ipv6
from ovirt_imageio._internal import qemu_img
from ovirt_imageio._internal import qemu_nbd
from ovirt_imageio._internal import server

from . import testutil

CLUSTER_SIZE = 64 * 1024
IMAGE_SIZE = 3 * CLUSTER_SIZE


@pytest.fixture(scope="module")
def srv():
    cfg = config.load(["test/conf/daemon.conf"])
    s = server.Server(cfg)
    s.start()
    yield s
    s.stop()


def prepare_transfer(srv, url, sparse=True, size=IMAGE_SIZE):
    ticket = testutil.create_ticket(
        url=url,
        size=size,
        sparse=sparse,
        ops=["read", "write"])

    srv.auth.add(ticket)

    host, port = srv.remote_service.address
    host = ipv6.quote_address(host)
    return "https://{}:{}/images/{}".format(host, port, ticket["uuid"])


class FakeProgress:

    def __init__(self):
        self.size = None
        self.updates = []

    def update(self, n):
        self.updates.append(n)


# TODO:
# - verify that upload optimized the upload using unix socket. Need a way to
#   enable only OPTIONS on the remote server.
# - verify that upload fall back to HTTPS if server does not support unix
#   socket. We don't have a way to disable unix socket currently.
# - verify that upload fall back to HTTPS if server support unix socket but is
#   not the local host. Probbly not feasble for these tests, unless we can
#   start a daemon on another host.
# - Test negative flows


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_empty_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst)

    client.upload(src, url, srv.config.tls.ca_file)

    # TODO: Check why allocation differ when src is qcow2. Target image
    # allocation is 0 bytes as expected, but comparing with strict=True fail at
    # offset 0.
    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_hole_at_start_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    with qemu_nbd.open(src, fmt) as c:
        c.write(IMAGE_SIZE - 6, b"middle")
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_hole_at_middle_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"start")
        c.write(IMAGE_SIZE - 3, b"end")
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_hole_at_end_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"start")
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, format1=fmt, format2="raw", strict=fmt == "raw")


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_full_sparse(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    with qemu_nbd.open(src, fmt) as c:
        c.write(0, b"b" * IMAGE_SIZE)
        c.flush()

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst, strict=True)


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_upload_preallocated(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    qemu_img.create(src, fmt, size=IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.write(b"a" * IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst, sparse=False)

    client.upload(src, url, srv.config.tls.ca_file)

    qemu_img.compare(src, dst)
    assert os.stat(dst).st_blocks * 512 == IMAGE_SIZE


@pytest.mark.parametrize("fmt,compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_upload_from_ova(tmpdir, srv, fmt, compressed):
    offset = CLUSTER_SIZE
    data = b"I can eat glass and it doesn't hurt me."

    # Create raw disk with some data.
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.seek(offset)
        f.write(data)

    # Create source disk.
    src = str(tmpdir.join("src"))
    qemu_img.convert(tmp, src, "raw", fmt, compressed=compressed)

    # Create placeholder ovf file.
    ovf = str(tmpdir.join("vm.ovf"))
    with open(ovf, "w") as f:
        f.write("<xml/>")

    # Create OVA package.
    ova = str(tmpdir.join("src.ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(ovf, arcname=os.path.basename(ovf))
        tar.add(src, arcname=os.path.basename(src))

    # Prepare destination file.
    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    # Test uploading src from ova.
    url = prepare_transfer(srv, "file://" + dst)
    client.upload(
        ova,
        url,
        srv.config.tls.ca_file,
        member=os.path.basename(src))

    qemu_img.compare(src, dst)


def test_upload_no_backing_chain(srv, nbd_server, tmpdir):
    size = CLUSTER_SIZE * 2

    # Create base image with some data in first cluster.
    base = str(tmpdir.join("base.qcow2"))
    qemu_img.create(base, "qcow2", size=size)
    with qemu_nbd.open(base, "qcow2") as c:
        c.write(0, b"data from base")
        c.flush()

    # Create src image with some data in second cluster.
    top = str(tmpdir.join("top.qcow2"))
    qemu_img.create(top, "qcow2", backing_file=base, backing_format="qcow2")
    with qemu_nbd.open(top, "qcow2") as c:
        c.write(CLUSTER_SIZE, b"data from top")
        c.flush()

    # Create empty destination image.
    dst = str(tmpdir.join("dst.qcow2"))
    qemu_img.create(dst, "qcow2", size=size)

    # Start nbd server for for destination image.
    nbd_server.image = dst
    nbd_server.fmt = "qcow2"
    nbd_server.start()

    # Upload using nbd backend.
    url = prepare_transfer(srv, nbd_server.sock.url(), size=size)
    client.upload(
        top,
        url,
        srv.config.tls.ca_file,
        backing_chain=False)

    # Stop the server to allow comparing.
    nbd_server.stop()

    # To compare top, we need to remove its backing file.
    qemu_img.unsafe_rebase(top, "")

    # Uploading without backing chain uploads data only from top image, and
    # does not zero unallocated area.
    qemu_img.compare(top, dst, format1="qcow2", format2="qcow2", strict=True)


@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_download_raw(tmpdir, srv, fmt):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)
        f.seek(IMAGE_SIZE // 2)
        f.write(b"data")

    url = prepare_transfer(srv, "file://" + src)
    dst = str(tmpdir.join("dst"))

    # When we download raw data, we can convert it on-the-fly to other format.
    client.download(url, dst, srv.config.tls.ca_file, fmt=fmt)

    # file backend does not support extents, so downloaded data is always
    # fully allocated.
    qemu_img.compare(src, dst, format1="raw", format2=fmt)


def test_download_qcow2_as_raw(tmpdir, srv):
    src = str(tmpdir.join("src.qcow2"))
    qemu_img.create(src, "qcow2", size=IMAGE_SIZE)

    # Allocate one cluster in the middle of the image.
    with qemu_nbd.open(src, "qcow2") as c:
        c.write(CLUSTER_SIZE, b"a" * CLUSTER_SIZE)
        c.flush()

    actual_size = os.path.getsize(src)
    url = prepare_transfer(srv, "file://" + src, size=actual_size)
    dst = str(tmpdir.join("dst.qcow2"))

    # When downloading qcow2 image using the nbd backend, we get raw data and
    # we can convert it to any format we want. Howver when downloading using
    # the file backend, we get qcow2 bytestream and we cannot convert it.
    #
    # To store the qcow2 bytestream, we must use fmt="raw". This instructs
    # qemu-nbd on the client side to treat the data as raw bytes, storing them
    # without any change on the local file.
    #
    # This is baisically like:
    #
    #   qemu-img convert -f raw -O raw src.qcow2 dst.qcow2
    #
    client.download(url, dst, srv.config.tls.ca_file, fmt="raw")

    # The result should be identical qcow2 image content. Allocation may
    # differ but for this test we get identical allocation.
    qemu_img.compare(src, dst, format1="qcow2", format2="qcow2", strict=True)


def test_upload_proxy_url(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    # If transfer_url is not accessible, proxy_url is used.
    transfer_url = "https://no.server:54322/images/no-ticket"
    proxy_url = prepare_transfer(srv, "file://" + dst)

    client.upload(src, transfer_url, srv.config.tls.ca_file,
                  proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw", strict=True)


def test_upload_proxy_url_unused(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    # If transfer_url is accessible, proxy_url is not used.
    transfer_url = prepare_transfer(srv, "file://" + dst)
    proxy_url = "https://no.proxy:54322/images/no-ticket"

    client.upload(src, transfer_url, srv.config.tls.ca_file,
                  proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw", strict=True)


def test_download_proxy_url(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))

    # If transfer_url is not accessible, proxy_url is used.
    transfer_url = "https://no.server:54322/images/no-ticket"
    proxy_url = prepare_transfer(srv, "file://" + src)

    client.download(transfer_url, dst, srv.config.tls.ca_file, fmt="raw",
                    proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw")


def test_download_proxy_url_unused(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))

    # If transfer_url is accessible, proxy_url is not used.
    transfer_url = prepare_transfer(srv, "file://" + src)
    proxy_url = "https://no.proxy:54322/images/no-ticket"

    client.download(transfer_url, dst, srv.config.tls.ca_file, fmt="raw",
                    proxy_url=proxy_url)

    qemu_img.compare(src, dst, format1="raw", format2="raw")


def test_progress(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.write(b"b" * 4096)
        f.seek(IMAGE_SIZE // 2)
        f.write(b"b" * 4096)
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst, sparse=True)

    progress = FakeProgress()
    client.upload(
        src, url, srv.config.tls.ca_file, progress=progress)

    assert progress.size == IMAGE_SIZE

    # Note: when using multiple connections order of updates is not
    # predictable.
    assert set(progress.updates) == {
        # First write.
        4096,
        # First zero.
        IMAGE_SIZE // 2 - 4096,
        # Second write.
        4096,
        # Second zero
        IMAGE_SIZE // 2 - 4096,
    }


def test_progress_callback(tmpdir, srv):
    src = str(tmpdir.join("src"))
    with open(src, "wb") as f:
        f.truncate(IMAGE_SIZE)

    dst = str(tmpdir.join("dst"))
    with open(dst, "wb") as f:
        f.truncate(IMAGE_SIZE)

    url = prepare_transfer(srv, "file://" + dst, size=IMAGE_SIZE, sparse=True)

    progress = []
    client.upload(
        src,
        url,
        srv.config.tls.ca_file,
        progress=progress.append)

    assert progress == [IMAGE_SIZE]


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_info(tmpdir, fmt, compressed):
    # Created temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Created test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)
    img_info = client.info(img)

    # Check image info.
    assert img_info["format"] == fmt
    assert img_info["virtual-size"] == size

    # We don't add member info if member was not specified.
    assert "member-offset" not in img_info
    assert "member-size" not in img_info

    # Create ova with test image.
    member = os.path.basename(img)
    ova = str(tmpdir.join("ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(img, arcname=member)

    # Get info for the member from the ova.
    ova_info = client.info(ova, member=member)

    # Image info from ova should be the same.
    assert ova_info["format"] == fmt
    assert ova_info["virtual-size"] == size

    # If member was specified, we report also the offset and size.
    with tarfile.open(ova) as tar:
        member_info = tar.getmember(member)
    assert ova_info["member-offset"] == member_info.offset_data
    assert ova_info["member-size"] == member_info.size


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_measure_to_raw(tmpdir, fmt, compressed):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Created test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)

    measure = client.measure(img, "raw")
    assert measure["required"] == size


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_measure_to_qcow2(tmpdir, fmt, compressed):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Created test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)

    measure = client.measure(img, "qcow2")
    assert measure["required"] == 393216


@pytest.mark.parametrize("compressed", [False, True])
@pytest.mark.parametrize("fmt", ["raw", "qcow2"])
def test_measure_from_ova(tmpdir, compressed, fmt):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Created test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", "qcow2", compressed=compressed)

    # Measure the image.
    img_measure = client.measure(img, fmt)

    # We don't add member info if member was not specified.
    assert "member-offset" not in img_measure
    assert "member-size" not in img_measure

    # Add test image to ova.
    member = os.path.basename(img)
    ova = str(tmpdir.join("ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(img, arcname=member)

    # Measure the image from the ova.
    ova_measure = client.measure(ova, fmt, member=member)

    # Measurement from ova should be same.
    assert ova_measure["required"] == img_measure["required"]
    assert ova_measure["fully-allocated"] == img_measure["fully-allocated"]

    # If member was specified, we report also the offset and size.
    with tarfile.open(ova) as tar:
        member_info = tar.getmember(member)
    assert ova_measure["member-offset"] == member_info.offset_data
    assert ova_measure["member-size"] == member_info.size


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_checksum(tmpdir, fmt, compressed):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Create test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)

    expected = blkhash.checksum(tmp, block_size=1024**2)
    actual = client.checksum(img, block_size=1024**2)
    assert actual == expected


@pytest.mark.parametrize("fmt, compressed", [
    ("raw", False),
    ("qcow2", False),
    ("qcow2", True),
])
def test_checksum_from_ova(tmpdir, fmt, compressed):
    # Create temporary file with some data.
    size = 2 * 1024**2
    tmp = str(tmpdir.join("tmp"))
    with open(tmp, "wb") as f:
        f.truncate(size)
        f.write(b"x" * CLUSTER_SIZE)

    # Create test image from temporary file.
    img = str(tmpdir.join("img"))
    qemu_img.convert(tmp, img, "raw", fmt, compressed=compressed)

    # Add test image to ova.
    member = os.path.basename(img)
    ova = str(tmpdir.join("ova"))
    with tarfile.open(ova, "w") as tar:
        tar.add(img, arcname=member)

    expected = blkhash.checksum(tmp, block_size=1024**2)
    actual = client.checksum(ova, member=member, block_size=1024**2)
    assert actual == expected


@pytest.mark.parametrize("algorithm,digest_size", [
    ("blake2b", 32),
    ("sha1", None),
])
def test_checksum_algorithm(tmpdir, algorithm, digest_size):
    img = str(tmpdir.join("img"))
    qemu_img.create(img, "raw", size="2m")

    expected = blkhash.checksum(
        img, block_size=1024**2, algorithm=algorithm, digest_size=digest_size)
    actual = client.checksum(img, block_size=1024**2, algorithm=algorithm)
    assert actual == expected
