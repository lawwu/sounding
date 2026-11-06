import io

from sounding import progress


class Tty(io.StringIO):
    def isatty(self):
        return True


def test_bar_draws_counts_and_keeps_log_lines_above_it():
    out = Tty()
    bar = progress.for_terminal("harvesting", out)
    assert isinstance(bar, progress.Bar)
    bar.add(3)
    bar.log("p.d: 3 tables")
    bar.advance(3)
    bar.close()
    text = out.getvalue()
    assert "p.d: 3 tables\n" in text and "3/3" in text and "harvesting [" in text
    assert text.endswith("\r\033[K")  # the bar is erased when done


def test_pipes_get_plain_lines_and_no_bar():
    out = io.StringIO()
    quiet = progress.for_terminal("harvesting", out)
    assert type(quiet) is progress.Quiet
    quiet.add(5)
    quiet.advance(5)
    quiet.log("p.d: 3 tables")
    quiet.close()
    assert out.getvalue() == "p.d: 3 tables\n"
