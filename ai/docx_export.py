"""Export stock analysis reports to Word (.docx) format."""

import logging
from datetime import datetime
from pathlib import Path

from docx import Document
from docx.shared import Inches, Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT

from config.settings import CHART_OUTPUT_DIR

logger = logging.getLogger(__name__)

# Style constants
HEADER_BG = RGBColor(0x1A, 0x56, 0xDB)  # Blue header
ALT_ROW_BG = RGBColor(0xF0, 0xF4, 0xFF)  # Light blue alternating rows
FONT_NAME_CN = "微软雅黑"


class DocxExporter:
    """Generate Word documents from stock analysis data."""

    def __init__(self, output_dir: Path | None = None):
        self.output_dir = output_dir or Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _set_cell_shading(self, cell, color: RGBColor):
        """Set cell background color."""
        from docx.oxml.ns import qn
        from docx.oxml import OxmlElement

        shading = OxmlElement("w:shd")
        shading.set(qn("w:fill"), f"{color}")
        shading.set(qn("w:val"), "clear")
        cell._tc.get_or_add_tcPr().append(shading)

    def _add_styled_table(self, doc: Document, headers: list[str], rows: list[list[str]]):
        """Add a table with blue header and alternating row colors."""
        table = doc.add_table(rows=1 + len(rows), cols=len(headers))
        table.alignment = WD_TABLE_ALIGNMENT.CENTER

        # Header row
        for i, header in enumerate(headers):
            cell = table.rows[0].cells[i]
            cell.text = header
            for paragraph in cell.paragraphs:
                paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
                for run in paragraph.runs:
                    run.font.bold = True
                    run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
                    run.font.size = Pt(10)
            self._set_cell_shading(cell, HEADER_BG)

        # Data rows
        for row_idx, row_data in enumerate(rows):
            for col_idx, value in enumerate(row_data):
                cell = table.rows[row_idx + 1].cells[col_idx]
                cell.text = str(value)
                for paragraph in cell.paragraphs:
                    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    for run in paragraph.runs:
                        run.font.size = Pt(9)
                if row_idx % 2 == 1:
                    self._set_cell_shading(cell, ALT_ROW_BG)

        return table

    def _fmt_value(self, val, suffix: str = "", decimals: int = 2) -> str:
        """Format a value for display, handling None/0."""
        if val is None:
            return "N/A"
        if isinstance(val, (int, float)):
            if val == 0 and suffix != "%":
                return "N/A"
            return f"{val:.{decimals}f}{suffix}"
        return str(val)

    def _fmt_market_cap(self, val) -> str:
        """Format market cap in 亿/万亿."""
        if not val or val == 0:
            return "N/A"
        if val > 1e12:
            return f"{val / 1e12:.1f}万亿"
        if val > 1e8:
            return f"{val / 1e8:.1f}亿"
        return f"{val / 1e4:.1f}万"

    def generate_stock_report(
        self,
        symbol: str,
        name: str,
        quote: dict,
        fundamental: dict,
        signals: dict,
        factor_scores: dict[str, float] | None = None,
        ai_commentary: str = "",
        technical_chart_path: str | None = None,
        radar_chart_path: str | None = None,
    ) -> str:
        """Generate a complete stock analysis Word document.

        Args:
            symbol: stock code e.g. "002795"
            name: stock name e.g. "永和智控"
            quote: realtime quote dict from fetcher
            fundamental: financial data dict from fetcher
            signals: technical signals dict from TechnicalAnalyzer
            factor_scores: factor scores dict for radar chart
            ai_commentary: AI-generated commentary text
            technical_chart_path: path to technical dashboard PNG
            radar_chart_path: path to radar chart PNG

        Returns:
            path to generated .docx file
        """
        doc = Document()

        # Title
        title = doc.add_heading(f"{name}（{symbol}）量化分析报告", level=0)
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER

        # Date subtitle
        date_para = doc.add_paragraph()
        date_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = date_para.add_run(f"报告日期：{datetime.now().strftime('%Y年%m月%d日')}")
        run.font.size = Pt(11)
        run.font.color.rgb = RGBColor(0x66, 0x66, 0x66)

        # --- Section 1: Realtime Quote ---
        doc.add_heading("一、实时行情", level=1)

        price = quote.get("price", 0)
        change_pct = quote.get("change_pct", 0)
        change_str = f"+{change_pct:.2f}%" if change_pct >= 0 else f"{change_pct:.2f}%"
        volume = quote.get("volume", 0)
        volume_str = f"{volume / 10000:.1f}万手" if volume > 10000 else f"{volume:.0f}手"
        amount = quote.get("amount", 0)
        amount_str = f"{amount / 1e8:.2f}亿" if amount > 1e8 else f"{amount / 1e4:.1f}万"

        quote_rows = [
            ["最新价", f"¥{price:.2f}", "涨跌幅", change_str],
            ["今开", self._fmt_value(quote.get("open")), "最高", self._fmt_value(quote.get("high"))],
            ["最低", self._fmt_value(quote.get("low")), "昨收", self._fmt_value(quote.get("prev_close"))],
            ["成交量", volume_str, "成交额", amount_str],
            ["换手率", self._fmt_value(quote.get("turnover_rate"), "%"), "总市值", self._fmt_market_cap(quote.get("total_market_cap"))],
        ]
        self._add_styled_table(doc, ["指标", "数值", "指标", "数值"], quote_rows)

        # --- Section 2: Financial Metrics ---
        doc.add_heading("二、核心财务指标", level=1)

        fd = fundamental or {}
        fin_rows = [
            ["PE(TTM)", self._fmt_value(fd.get("pe_ttm")), "PB(MRQ)", self._fmt_value(fd.get("pb"))],
            ["PS(TTM)", self._fmt_value(fd.get("ps_ttm")), "ROE", self._fmt_value(fd.get("roe"), "%")],
            ["毛利率", self._fmt_value(fd.get("gross_margin"), "%"), "净利率", self._fmt_value(fd.get("net_margin"), "%")],
            ["营收增长", self._fmt_value(fd.get("revenue_growth"), "%"), "利润增长", self._fmt_value(fd.get("profit_growth"), "%")],
            ["EPS", self._fmt_value(fd.get("eps"), "元"), "BPS", self._fmt_value(fd.get("bps"), "元")],
            ["资产负债率", self._fmt_value(fd.get("debt_ratio"), "%"), "流动比率", self._fmt_value(fd.get("current_ratio"))],
        ]
        self._add_styled_table(doc, ["指标", "数值", "指标", "数值"], fin_rows)

        # --- Section 3: Technical Analysis ---
        doc.add_heading("三、技术分析", level=1)

        signal_rows = []
        label_map = {
            "macd_signal": "MACD",
            "rsi_signal": "RSI",
            "kdj_signal": "KDJ",
            "boll_signal": "布林带",
            "ma_alignment": "均线排列",
        }
        for key, label in label_map.items():
            if key in signals:
                signal_rows.append([label, str(signals[key])])

        score = signals.get("score", "N/A")
        overall = signals.get("overall", "N/A")
        signal_rows.append(["综合评分", f"{score}"])
        signal_rows.append(["综合研判", str(overall)])

        self._add_styled_table(doc, ["技术指标", "信号"], signal_rows)

        if technical_chart_path and Path(technical_chart_path).exists():
            doc.add_paragraph()
            doc.add_picture(technical_chart_path, width=Inches(6))
            last_para = doc.paragraphs[-1]
            last_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

        # --- Section 4: Factor Scores ---
        if factor_scores:
            doc.add_heading("四、多因子量化评分", level=1)

            factor_rows = [[k, f"{v:.1f}"] for k, v in factor_scores.items()]
            avg_score = sum(factor_scores.values()) / len(factor_scores) if factor_scores else 0
            factor_rows.append(["综合得分", f"{avg_score:.1f}"])
            self._add_styled_table(doc, ["因子维度", "得分(0-100)"], factor_rows)

            if radar_chart_path and Path(radar_chart_path).exists():
                doc.add_paragraph()
                doc.add_picture(radar_chart_path, width=Inches(5))
                last_para = doc.paragraphs[-1]
                last_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

        # --- Section 5: AI Commentary ---
        if ai_commentary:
            section_num = "五" if factor_scores else "四"
            doc.add_heading(f"{section_num}、AI智能分析研判", level=1)

            for line in ai_commentary.split("\n"):
                line = line.strip()
                if not line:
                    continue
                if line.startswith("##"):
                    doc.add_heading(line.lstrip("#").strip(), level=2)
                elif line.startswith("#"):
                    doc.add_heading(line.lstrip("#").strip(), level=2)
                elif line.startswith("- ") or line.startswith("* "):
                    doc.add_paragraph(line[2:], style="List Bullet")
                elif line.startswith("**") and line.endswith("**"):
                    p = doc.add_paragraph()
                    run = p.add_run(line.strip("*"))
                    run.bold = True
                else:
                    doc.add_paragraph(line)

        # --- Disclaimer ---
        doc.add_paragraph()
        disclaimer = doc.add_paragraph()
        disclaimer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = disclaimer.add_run(
            "免责声明：本报告由AI量化分析系统自动生成，仅供参考，不构成投资建议。"
            "投资有风险，决策需谨慎。"
        )
        run.font.size = Pt(8)
        run.font.color.rgb = RGBColor(0x99, 0x99, 0x99)
        run.italic = True

        # Save
        filename = f"{name}_{symbol}_量化分析报告.docx"
        filepath = self.output_dir / filename
        doc.save(str(filepath))
        logger.info(f"Report saved to {filepath}")
        return str(filepath)
