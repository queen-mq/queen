"""Every runner a test starts is stopped after it, passed or failed."""

import pytest_asyncio

from queen.streams.stream import Stream


@pytest_asyncio.fixture(autouse=True)
async def _stop_runners(monkeypatch):
    started = []
    original = Stream.run

    async def run(self, *args, **kwargs):
        runner = await original(self, *args, **kwargs)
        started.append(runner)
        return runner

    monkeypatch.setattr(Stream, "run", run)
    yield started
    while started:
        try:
            await started.pop().stop()
        except Exception:  # noqa: BLE001
            pass
