from __future__ import annotations
from typing import (
    Any,
    Dict,
    Mapping,
    List,
    Optional,
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
