from typing import List, Tuple
from fpdf import FPDF

# from datetime import datetime, timezone


def pdf(data: List[Tuple[str]], name: str = "cuallee.pdf"):
    # today = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    # style = FontFace(fill_color="#AAAAAA")
    pdf = FPDF(orientation="landscape", format="A4")
    pdf.add_page()
    pdf.set_font("Helvetica", size=6)
    irow = -1
    with pdf.table(
        borders_layout="SINGLE_TOP_LINE",
        width=270,
        col_widths=(15, 25, 25, 25, 115, 25, 25),
    ) as table:
        for data_row in data:
            row = table.row()
            for datum in data_row:
                row.cell(datum)
            irow += 1

    pdf.output(name)
