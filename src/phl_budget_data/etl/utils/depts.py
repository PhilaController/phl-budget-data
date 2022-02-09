from __future__ import annotations

import json

import rich.repr
from billy_penn.departments import load_city_departments
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

from ... import DATA_DIR


def merge_department_info(data, left_on="dept_name", right_on="alias"):
    """Merge department info."""

    # Add dept info
    dept_info = load_city_departments(include_aliases=True, include_line_items=True)
    data = data.merge(
        dept_info,
        left_on=left_on,
        right_on=right_on,
        how="left",
        validate="1:1",
        suffixes=("_raw", ""),
    )

    # Match missing departments
    data = match_missing_departments(data)

    return data


class OptionClick(Message, bubble=True):
    pass


@rich.repr.auto(angular=False)
class Option(Widget, can_focus=True):

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


def _divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i : i + n]


def launch_department_selector(missing_dept, results_per_page=10):
    """Launch a textual app to select the department."""

    # Save the result here
    results = []

    # Get the departments
    depts = load_city_departments(include_line_items=True)

    # Chunk the departments
    dept_names = list(_divide_chunks(sorted(depts["dept_name"]), results_per_page))
    total_pages = len(dept_names)

    class DepartmentSelectorApp(App):
        """Demonstrates custom widgets"""

        option_name: Reactive[str] = Reactive("")
        page: Reactive[int] = Reactive(0)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.options = [Option(name=dept) for dept in dept_names[self.page]]

        async def on_mount(self) -> None:
            await self.view.dock(
                Header(style="white on blue"), *self.options, edge="top"
            )

        async def on_load(self, event: events.Load) -> None:
            """Bind keys with the app loads (but before entering application mode)"""
            await self.bind("q", "quit", "Quit")
            await self.bind("right", "next")
            await self.bind("left", "prev")

        async def update_options(self):

            this_page = dept_names[self.page]
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
            results.append(message.sender.name)
            await self.action_quit()

    # Run
    DepartmentSelectorApp.run(title=f"Selector for: '{missing_dept}'")

    if len(results):
        match = results[0]
        sel = depts["dept_name"] == match
        return depts.loc[sel].squeeze()
    else:
        return None


def match_missing_departments(data):
    """Match missing departments."""

    # Make a copy
    data = data.copy()

    # Check for missing
    missing = data["alias"].isnull()
    if missing.sum():

        # Get the missing dept names and exclude general fund
        missing_depts = data.loc[missing]["dept_name_raw"].tolist()
        missing_depts = [d for d in missing_depts if "general fund" not in d.lower()]

        # Load the cached matches
        filename = DATA_DIR / "interim" / "dept-matches.json"
        with filename.open("r") as ff:
            cached_matches = json.load(ff)

        # Launch the selector
        if len(missing_depts):

            for missing_dept in missing_depts:

                if missing_dept in cached_matches:
                    matched_dept = cached_matches[missing_dept]
                else:
                    # Launch the selector
                    matched_dept = launch_department_selector(missing_dept)

                    # Raise an error if no match
                    if matched_dept is None:
                        raise ValueError(
                            f"Missing aliases for {len(missing_depts)} departments:\n{missing_depts}"
                        )
                    else:
                        matched_dept = matched_dept.to_dict()

                    # Save it
                    cached_matches[missing_dept] = matched_dept

                # Update the values
                sel = data["dept_name_raw"] == missing_dept
                for col, value in matched_dept.items():
                    data.loc[sel, col] = value

            # Save the cached matches
            with filename.open("w") as ff:
                json.dump(cached_matches, ff)

    return data
