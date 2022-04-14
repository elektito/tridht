import logging
import trio
from collections import defaultdict
from contextvars import ContextVar
from quart import jsonify
from quart_trio import QuartTrio
from .semaphore import all_semaphores


logger = logging.getLogger(__name__)
app = QuartTrio(__name__)
tracer_info = ContextVar('tracer')


class TaskInfo:
    def __init__(self, name):
        self.name = name


class Tracer(trio.abc.Instrument):
    def __init__(self):
        self.running = False
        self.live_tasks = []
        self.scheduled_tasks = []
        self.total_sleep_time = 0

        # list of TaskInfo objects for each dead task (so that we
        # won't have to keep a reference to the actual task objects)
        self.dead_tasks = []

        # task name => cpu time spent
        self.time_per_task_name = defaultdict(int)

        self._sleep_start_time = None
        self._cur_task_start_time = None

    @property
    def live_count_per_name(self):
        # returns a dictionary mapping task names to the number of
        # live tasks with that name
        ret = defaultdict(int)
        for task in self.live_tasks:
            ret[task.name] += 1
        return ret

    @property
    def dead_count_per_name(self):
        # returns a dictionary mapping task names to the number of
        # dead tasks with that name
        ret = defaultdict(int)
        for task_info in self.dead_tasks:
            ret[task_info.name] += 1
        return ret

    @property
    def scheduled_count_per_name(self):
        # returns a dictionary mapping task names to the number of
        # currently scheduled tasks with that name
        ret = defaultdict(int)
        for task_info in self.scheduled_tasks:
            ret[task_info.name] += 1
        return ret

    def before_run(self):
        self.running = True

    def task_spawned(self, task):
        self.live_tasks.append(task)

    def task_scheduled(self, task):
        self.scheduled_tasks.append(task)

    def before_task_step(self, task):
        self._cur_task_start_time = trio.current_time()

    def after_task_step(self, task):
        # if the instrument is added after trio.run is called, we
        # might have missed some 'before_task_step' invocations.
        if self._cur_task_start_time is not None:
            step_time = trio.current_time() - self._cur_task_start_time
            self.time_per_task_name[task.name] += step_time
            self._cur_task_start_time = None

        try:
            self.scheduled_tasks.remove(task)
        except ValueError:
            # task has exited
            pass

    def task_exited(self, task):
        try:
            self.live_tasks.remove(task)
        except ValueError:
            pass

        try:
            self.scheduled_tasks.remove(task)
        except ValueError:
            pass

        try:
            self.dead_tasks.append(TaskInfo(task.name))
        except ValueError:
            pass

    def before_io_wait(self, timeout):
        self._sleep_start_time = trio.current_time()

    def after_io_wait(self, timeout):
        duration = trio.current_time() - self._sleep_start_time
        self.total_sleep_time += duration

    def after_run(self):
        self.running = False


@app.route('/stats')
async def stats():
    tracer = tracer_info.get()

    stats = {
        'live_count': len(tracer.live_tasks),
        'dead_count': len(tracer.dead_tasks),
        'scheduled_count': len(tracer.scheduled_tasks),
        'total_sleep_time': tracer.total_sleep_time,
        'cpu_time_per_task': tracer.time_per_task_name,
        'live_count_per_name': tracer.live_count_per_name,
        'dead_count_per_name': tracer.dead_count_per_name,
        'scheduled_count_per_name': tracer.scheduled_count_per_name,
        'semaphores': {
            f'{"/".join(s.creation_stack)} - {s.qualname}': s.value
            for s in all_semaphores
        }
    }
    return jsonify(stats)


async def run_monitor(tracer, port=7100):
    logger.info(f'Starting monitor on port {port}...')
    tracer_info.set(tracer)
    await app.run_task(port=port)
