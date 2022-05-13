"""Module for launching textual app to select the department."""
from __future__ import annotations

from typing import Iterator, Optional

import rich.repr
from rich import box
from rich.align import Align
from rich.console import RenderableType
from rich.panel import Panel
from rich.pretty import Pretty
from textual import events
from textual.app import App
from textual.message import Message
from textual.widget import Reactive, Widget
from textual.widgets import Header


class OptionClick(Message, bubble=True):  # type: ignore
    pass


@rich.repr.auto(angular=False)
class Option(Widget, can_focus=True):  # type: ignore

    has_focus: Reactive[bool] = Reactive(False)
    mouse_over: Reactive[bool] = Reactive(False)
    style: Reactive[str] = Reactive("")
    height: Reactive[int | None] = Reactive(None)

    def __init__(self, *, name: str | None = None, height: int | None = None) -> None:
        super().__init__(name=name)
        self.height = height

    def render(self) -> RenderableType:
        return Panel(
            Align.center(
                Pretty(self, no_wrap=True, overflow="ellipsis"), vertical="middle"
            ),
            title="Department",
            border_style="green" if self.mouse_over else "blue",
            box=box.HEAVY if self.has_focus else box.ROUNDED,
            style=self.style,
            height=self.height,
        )

    async def on_focus(self, event: events.Focus) -> None:
        self.has_focus = True
        event.prevent_default().stop()
        await self.emit(OptionClick(self))

    async def on_blur(self, event: events.Blur) -> None:
        self.has_focus = False

    async def on_enter(self, event: events.Enter) -> None:
        self.mouse_over = True

    async def on_leave(self, event: events.Leave) -> None:
        self.mouse_over = False


def launch_selector(
    options: list[str], value: str, results_per_page: int = 10
) -> Optional[str]:
    """Launch a textual app to select the department."""

    # Save the results here
    results = []

    # Divide list of string into list of list
    def paginate(l: list[str], n: int) -> Iterator[list[str]]:
        for i in range(0, len(l), n):
            yield l[i : i + n]

    # Chunk the departments
    option_pages = list(paginate(options, results_per_page))
    total_pages = len(option_pages)

    class BestMatchSelectorApp(App):
        """Textual app to select the best match for an input value."""

        option_name: Reactive[str] = Reactive("")
        page: Reactive[int] = Reactive(0)

        def __init__(self, *args, **kwargs):  # type: ignore
            super().__init__(*args, **kwargs)
            self.options = [Option(name=option) for option in option_pages[self.page]]

        async def on_mount(self) -> None:
            await self.view.dock(
                Header(style="white on blue"), *self.options, edge="top"
            )

        async def on_load(self, event: events.Load) -> None:
            """Bind keys with the app loads (but before entering application mode)"""
            await self.bind("q", "quit", "Quit")
            await self.bind("right", "next")
            await self.bind("left", "prev")

        async def update_options(self) -> None:
            """Update the options to display."""

            this_page = option_pages[self.page]
            for i in range(results_per_page):

                if i < len(this_page):
                    self.options[i].name = this_page[i]
                    self.options[i].visible = True
                else:
                    self.options[i].visible = False
                self.options[i].refresh()

            await self.view.refresh_layout()

        async def action_prev(self) -> None:
            if self.page > 0:
                self.page -= 1
            await self.update_options()

        async def action_next(self) -> None:
            if self.page < total_pages - 1:
                self.page += 1
            await self.update_options()

        async def handle_option_click(self, message: OptionClick) -> None:
            """A message sent by the option widget"""

            assert isinstance(message.sender, Option)
            results.append(message.sender.name)  # Save the option
            await self.action_quit()

    # Run
    BestMatchSelectorApp.run(title=f"Selector for: '{value}'")

    # Did we get a match?
    if len(results):
        return results[0]
    else:
        return None
