from typing import List, Tuple
from fpdf import FPDF, FontFace
from datetime import datetime, timezone

def pdf(data: List[Tuple[str]], name: str = "cuallee.pdf"):
    today = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    style = FontFace(fill_color=rgb_7)
    pdf = FPDF(orientation="landscape", format="A4")
    pdf.add_page()
    pdf.set_font("Berkeley", size=6)
    irow = -1
    with pdf.table(borders_layout="SINGLE_TOP_LINE", width=270, col_widths=(15, 25, 25, 25, 115, 25, 25)) as table:
        for data_row in data:
            row = table.row()
            for datum in data_row:
                # if irow in top_5:
                #     row.cell(datum, style=style)
                # else:
                #    row.cell(datum)
                row.cell(datum)
            irow += 1


