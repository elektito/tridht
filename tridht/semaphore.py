import trio
import traceback


all_semaphores = []


class Semaphore:
    """This is a wrapper around trio.Semaphore. We use it to be able to
    keep track of all existing semaphores and possibly report them in
    the monitor.

    """

    _index = 0

    def __init__(self, initial_value, *, max_value=None, name=None):
        self._sem = trio.Semaphore(initial_value, max_value=max_value)
        self._initial_value = initial_value
        all_semaphores.append(self)

        self.name = name

        # index is unique to each semaphore and can be used to
        # distinguish semaphores with the same name (for example ones
        # stored in locals of a coro entered many times)
        self.index = Semaphore._index
        Semaphore._index += 1

        # passing None to walk_stack causes it to return stack frames
        # starting from the caller, which is what we want here. If we
        # wanted current frame to also be included, we could pass
        # inspect.currentframe instead.
        self.creation_stack = []
        for frame, lineno in traceback.walk_stack(None):
            self.creation_stack.append(frame.f_code.co_name)

            # prevent circular references
            del frame
        self.creation_stack.reverse()

    def __del__(self):
        all_semaphores.remove(self)

    def __str__(self):
        if self._sem.max_value is not None:
            maxval = self.max_value
        else:
            maxval = 'unbounded'
        return (
            f'Semaphore({self.value}/{maxval}, '
            f'initial={self._initial_value})'
        )

    async def acquire(self):
        await self._sem.acquire()

    def acquire_nowait(self):
        self._sem.acquire_nowait()

    def release(self):
        self._sem.release()

    @property
    def max_value(self):
        return self._sem.max_value

    @property
    def value(self):
        return self._sem.value

    @property
    def qualname(self):
        return f'{self.name}-{self.index}'
