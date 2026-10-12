"""Build static documentation charts from the existing benchmark CSV files."""

import csv
import re
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from statistics import median

from reportlab.graphics import renderSVG
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.shapes import Circle, Drawing, Line, Rect, String
from reportlab.graphics.widgets.markers import makeMarker
from reportlab.lib.colors import HexColor, white

ROOT = Path(__file__).resolve().parent.parent
ASSETS = ROOT / "docs/assets"
COLORS = {"Silver": HexColor("#2563eb"), "Gold": HexColor("#0f766e")}
INK = HexColor("#19334c")
MUTED = HexColor("#526477")


def read_csv(name):
    with (ROOT / "benchmarks" / name).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def french(value):
    return str(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)).replace(".", ",")


def text(drawing, x, y, value, size=13, color=INK, anchor="start"):
    drawing.add(String(x, y, value, fontName="Helvetica", fontSize=size,
                       fillColor=color, textAnchor=anchor))


def canvas(title, subtitle):
    drawing = Drawing(1040, 440)
    drawing.add(Rect(0, 0, 1040, 440, fillColor=white, strokeColor=None))
    text(drawing, 36, 408, title, 23)
    text(drawing, 36, 382, subtitle, 13, MUTED)
    return drawing


def style_plot(plot, maximum):
    plot.xValueAxis.labels.fontName = plot.yValueAxis.labels.fontName = "Helvetica"
    plot.xValueAxis.labels.fontSize = plot.yValueAxis.labels.fontSize = 12
    plot.xValueAxis.strokeColor = plot.yValueAxis.strokeColor = HexColor("#b6c3cf")
    plot.yValueAxis.visibleGrid = True
    plot.yValueAxis.gridStrokeColor = HexColor("#e5ebf0")
    plot.yValueAxis.valueMin = 0
    plot.yValueAxis.valueMax = maximum
    plot.yValueAxis.valueStep = 50


def marker(name, color, size):
    symbol = makeMarker(name)
    symbol.fillColor, symbol.strokeColor, symbol.size = color, color, size
    return symbol


def nyc_chart(rows):
    if len(rows) != 6:
        raise ValueError("NYC requires six measurements")
    drawing = canvas("NYC TLC · durée des CLI", "Trois exécutions par phase · travaux différents, pas deux moteurs à comparer")
    expected = {"Silver": ["187.51", "152.19", "188.08"], "Gold": ["44.70", "38.28", "53.50"]}
    for phase, left in [("Silver", 78), ("Gold", 584)]:
        series = sorted((row for row in rows if row["phase"] == phase), key=lambda row: int(row["run"]))
        if [row["run"] for row in series] != ["1", "2", "3"]:
            raise ValueError(f"Incomplete NYC series: {phase}")
        values = [Decimal(row["elapsed_seconds"]) for row in series]
        mid, low, high = median(values), min(values), max(values)
        rounded = [str(value.quantize(Decimal("0.01"))) for value in [mid, low, high]]
        if rounded != expected[phase]:
            raise ValueError(f"Unexpected NYC statistics: {phase}: {rounded}")
        print(f"NYC {phase}: median={mid}, min={low}, max={high}")
        color = COLORS[phase]
        text(drawing, left, 354, phase, 18, color)
        text(drawing, left, 334, "Durée totale (secondes)", 11, MUTED)
        plot = LinePlot()
        plot.x, plot.y, plot.width, plot.height = left, 140, 398, 178
        style_plot(plot, 220)
        plot.xValueAxis.valueMin, plot.xValueAxis.valueMax = 0.5, 3.5
        plot.xValueAxis.valueSteps = [1, 2, 3]
        plot.data = [[(index + 1, float(value)) for index, value in enumerate(values)],
                     [(0.6, float(mid)), (3.4, float(mid))]]
        plot.lines[0].strokeColor = None
        plot.lines[0].symbol = marker("FilledCircle", color, 8)
        plot.lines[1].strokeColor = color
        plot.lines[1].strokeDashArray = [5, 4]
        plot.lines[1].strokeWidth = 1.5
        drawing.add(plot)
        for index, value in enumerate(values):
            x = left + (index + 0.5) / 3 * 398
            y = 140 + float(value) / 220 * 178
            text(drawing, x, y + 13, french(value), 12, color, "middle")
        text(drawing, left + 199, 110, "Exécution", 12, MUTED, "middle")
        text(drawing, left, 80, f"Médiane {french(mid)} s · min {french(low)} · max {french(high)}", 13)
    drawing.add(Circle(83, 46, 4, fillColor=INK, strokeColor=None))
    text(drawing, 95, 42, "Mesure individuelle", 12)
    for start in [280, 292, 304]:
        drawing.add(Line(start, 46, start + 7, 46, strokeColor=INK))
    text(drawing, 327, 42, "Médiane des trois mesures", 12)
    text(drawing, 36, 16, "Source : benchmarks/nyc_cli_measurements_20261012.csv · WSL /mnt/c · démarrage Spark inclus", 11, MUTED)
    return drawing


def synthetic_chart(rows):
    if len(rows) != 4:
        raise ValueError("Synthetic benchmarks require four series")
    drawing = canvas("Synthétique · durée des CLI", "Mesures manuelles fournies · trois exécutions par volume et par phase")
    plot = LinePlot()
    plot.x, plot.y, plot.width, plot.height = 205, 130, 545, 212
    style_plot(plot, 4.5)
    plot.yValueAxis.valueMin = 0.5
    plot.yValueAxis.visible = False
    plot.xValueAxis.valueMin, plot.xValueAxis.valueMax, plot.xValueAxis.valueStep = 0, 80, 20
    ordered = sorted(rows, key=lambda row: (int(row["base_transactions"]), row["phase"] != "Silver"))
    plot.data = []
    summaries = []
    for index, row in enumerate(ordered):
        values = [Decimal(row[f"run{run}_seconds"]) for run in range(1, 4)]
        mid, low, high = [Decimal(row[name]) for name in ["median_seconds", "min_seconds", "max_seconds"]]
        if [mid, low, high] != [median(values), min(values), max(values)]:
            raise ValueError("Synthetic CSV statistics do not reconcile")
        y, color = 4 - index, COLORS[row["phase"]]
        plot.data.extend([[(float(low), y), (float(high), y)], [(float(mid), y)]])
        plot.lines[2 * index].strokeColor = color
        plot.lines[2 * index].strokeWidth = 3
        plot.lines[2 * index].symbol = marker("FilledCircle", color, 5)
        plot.lines[2 * index + 1].strokeColor = None
        plot.lines[2 * index + 1].symbol = marker("FilledDiamond", color, 12)
        summaries.append((row, mid, low, high, y))
        print(f"Synthetic {row['phase']} {row['base_transactions']}: median={mid}, min={low}, max={high}")
    drawing.add(plot)
    text(drawing, 785, 350, "Médiane [min – max] (s)", 12, MUTED)
    for row, mid, low, high, y in summaries:
        screen_y = 130 + (y - 0.5) / 4 * 212
        volume = "100k" if row["base_transactions"] == "100000" else "1m"
        text(drawing, 36, screen_y - 4, f"{volume} · {row['phase']}", 15, COLORS[row["phase"]])
        text(drawing, 785, screen_y - 4, f"{french(mid)} [{french(low)} – {french(high)}]", 13)
    text(drawing, 477, 87, "Durée totale CLI (secondes)", 13, MUTED, "middle")
    text(drawing, 36, 48, "Losange : médiane · segment et extrémités : minimum / maximum", 12)
    text(drawing, 36, 18, "Source : benchmarks/results.csv · WSL /mnt/c · démarrage Spark inclus · seed 42", 11, MUTED)
    return drawing


def save_svg(drawing, filename, title, description):
    path = ASSETS / filename
    renderSVG.drawToFile(drawing, str(path))
    svg = path.read_text(encoding="utf-8")
    # Keep readable text and local font fallbacks; no external DTD is needed.
    svg = re.sub(r"<!DOCTYPE svg.*?>", "", svg, flags=re.DOTALL)
    svg = svg.replace("font-family: Helvetica;", "font-family: Arial, Helvetica, sans-serif;")
    svg = svg.replace("<svg ", '<svg role="img" aria-labelledby="chart-title chart-desc" ', 1)
    svg = svg.replace("<title>...</title>", f'<title id="chart-title">{title}</title>')
    svg = svg.replace("<desc>...</desc>", f'<desc id="chart-desc">{description}</desc>')
    path.write_text(svg, encoding="utf-8")


if __name__ == "__main__":
    ASSETS.mkdir(parents=True, exist_ok=True)
    save_svg(nyc_chart(read_csv("nyc_cli_measurements_20261012.csv")), "nyc_cli_times.svg",
             "Temps NYC Silver et Gold", "Trois points par phase et médiane en secondes. Travaux différents, pas une comparaison de moteurs.")
    save_svg(synthetic_chart(read_csv("results.csv")), "synthetic_cli_times.svg",
             "Temps synthétiques 100k et 1m", "Médiane et intervalle minimum-maximum des trois mesures manuelles fournies, en secondes.")
