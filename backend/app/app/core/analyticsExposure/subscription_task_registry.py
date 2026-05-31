import asyncio

class SubscriptionTaskRegistry:
    """
    Registry for subscription tasks.
    It assumes single task for each subscription_id.
    It also allows to associate an event with the task, which can be used to signal the task (namely for db updates).
    """
    def __init__(self):
        self.__tasks: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}

    def register(self, subscription_id: str, *, task: asyncio.Task, event: asyncio.Event | None = None) -> None:
        """Register a new task. Single task for each subscription_id"""
        if subscription_id in self.__tasks:
            raise Exception(f"Subscription already registered for id {subscription_id}")
        self.__tasks[subscription_id] =  (task, event)
        task.add_done_callback(lambda _: self.__on_done_task(subscription_id, finished_task=task))

    def cancel(self, subscription_id: str) -> bool:
        """Cancel a task. Returns True if a task was found and cancelled."""
        task, _ = self.__tasks.get(subscription_id)
        if task and not task.done():
            task.cancel()
            return True
        return False

    def get(self, subscription_id: str) -> tuple[asyncio.Task, asyncio.Event] | None:
        return self.__tasks.get(subscription_id)

    def __on_done_task(self, subscription_id: str, *, finished_task: asyncio.Task) -> None:
        """OnDoneCallback to remove the task from the registry if it has not yet been replaced (never happens)"""
        if self.__tasks.get(subscription_id) is finished_task:
            self.__tasks.pop(subscription_id, None)


subscription_task_registry = SubscriptionTaskRegistry()

