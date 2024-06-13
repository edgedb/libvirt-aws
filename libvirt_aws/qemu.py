from __future__ import annotations
from typing import (
    Any,
    Dict,
    Mapping,
    List,
    Optional,
    Union,
)

import asyncio
import base64
import io
import json

import libvirt
import libvirt_qemu


class RemoteProcess:
    def __init__(
        self,
        pid: int,
        returncode: int,
        stdout: bytes,
        stderr: bytes,
    ) -> None:
        self._pid = pid
        self._returncode = returncode
        self._stdout = io.BytesIO(stdout)
        self._stderr = io.BytesIO(stderr)

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def returncode(self) -> int:
        return self._returncode

    @property
    def stdin(self) -> Optional[io.BytesIO]:
        return None

    @property
    def stdout(self) -> io.BytesIO:
        return self._stdout

    @property
    def stderr(self) -> io.BytesIO:
        return self._stderr


async def agent_exec(
    domain: libvirt.virDomain,
    args: List[str],
    *,
    env: Optional[Mapping[str, Any]] = None,
    timeout_sec: float = 5.0,
) -> RemoteProcess:
    command = {
        "execute": "guest-exec",
        "arguments": {
            "path": args[0],
            "arg": args[1:],
            "env": [f"{k}={v}" for k, v in env.items()] if env else [],
            "capture-output": True,
        },
    }

    result = await agent_command(domain, command)
    pid = result["pid"]

    async def _loop() -> RemoteProcess:
        while True:
            command = {
                "execute": "guest-exec-status",
                "arguments": {
                    "pid": pid,
                },
            }

            result = await agent_command(domain, command)

            if result["exited"]:
                out_b64 = result.get("out-data", "")
                out = base64.b64decode(out_b64) if out_b64 else b""
                err_b64 = result.get("err-data", "")
                err = base64.b64decode(err_b64) if err_b64 else b""
                return RemoteProcess(pid, result["exitcode"], out, err)
            else:
                await asyncio.sleep(0.1)

    return await asyncio.wait_for(_loop(), timeout=timeout_sec)


class RemoteFile:
    def __init__(self, domain: libvirt.virDomain, handle: int) -> None:
        self.domain = domain
        self.handle = handle


async def open_remote(
    domain: libvirt.virDomain,
    path: str,
    mode: str,
) -> RemoteFile:
    command = {
        "execute": "guest-file-open",
        "arguments": {
            "path": path,
            "mode": mode,
        },
    }
    handle = await agent_command(domain, command)
    return RemoteFile(domain, handle)


async def write_remote(
    handle: RemoteFile,
    buf: Union[bytes, bytearray, memoryview],
) -> int:
    command = {
        "execute": "guest-file-write",
        "arguments": {
            "handle": handle.handle,
            "buf-b64": base64.b64encode(buf).decode("ascii"),
            "count": len(buf),
        },
    }
    result = await agent_command(handle.domain, command)
    return result["count"]


async def close_remote(handle: RemoteFile) -> None:
    command = {
        "execute": "guest-file-close",
        "arguments": {
            "handle": handle.handle,
        },
    }

    await agent_command(handle.domain, command)


async def write_remote_text(
    domain: libvirt.virDomain,
    path: str,
    content: str,
) -> int:
    file = await open_remote(domain, path, "w")
    try:
        result = await write_remote(file, content.encode("utf-8"))
    finally:
        await close_remote(file)

    return result


async def agent_command(
    domain: libvirt.virDomain,
    command: Dict[str, Any],
) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    resp = await loop.run_in_executor(
        None,
        libvirt_qemu.qemuAgentCommand,
        domain,
        json.dumps(command),
        -1,
        0,
    )
    return json.loads(resp)["return"]  # type: ignore [no-any-return]
