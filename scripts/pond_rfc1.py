#!/usr/bin/env python3
"""
Pond RFC 1: Storage & Versioned State
Formal specification document, generated via ReportLab.
"""

import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white, Color
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.pdfmetrics import registerFontFamily
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle,
    KeepTogether, Image, HRFlowable, ListFlowable, ListItem,
    BaseDocTemplate, Frame, PageTemplate, NextPageTemplate
)
from reportlab.platypus.flowables import Flowable

# ---------------------------------------------------------------------------
# Font registration
# ---------------------------------------------------------------------------
FONT_DIR = '/usr/share/fonts'

pdfmetrics.registerFont(TTFont('NotoSerifSC',
    f'{FONT_DIR}/truetype/noto-serif-sc/NotoSerifSC-Regular.ttf'))
pdfmetrics.registerFont(TTFont('NotoSerifSC-Bold',
    f'{FONT_DIR}/truetype/noto-serif-sc/NotoSerifSC-Bold.ttf'))
pdfmetrics.registerFont(TTFont('NotoSerifSC-Medium',
    f'{FONT_DIR}/truetype/noto-serif-sc/NotoSerifSC-Medium.ttf'))
pdfmetrics.registerFont(TTFont('NotoSerifSC-Light',
    f'{FONT_DIR}/truetype/noto-serif-sc/NotoSerifSC-Light.ttf'))
registerFontFamily('NotoSerifSC',
    normal='NotoSerifSC', bold='NotoSerifSC-Bold',
    italic='NotoSerifSC', boldItalic='NotoSerifSC-Bold')

pdfmetrics.registerFont(TTFont('DejaVuMono',
    f'{FONT_DIR}/truetype/dejavu/DejaVuSansMono.ttf'))
pdfmetrics.registerFont(TTFont('DejaVuMono-Bold',
    f'{FONT_DIR}/truetype/dejavu/DejaVuSansMono-Bold.ttf'))
registerFontFamily('DejaVuMono',
    normal='DejaVuMono', bold='DejaVuMono-Bold',
    italic='DejaVuMono', boldItalic='DejaVuMono-Bold')

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------
PRIMARY    = HexColor('#0F172A')   # near-black slate
ACCENT     = HexColor('#0E7490')   # deep teal
ACCENT_SOFT= HexColor('#E0F7FA')   # very light teal
RULE       = HexColor('#475569')   # slate gray for rules
MUTED      = HexColor('#64748B')   # muted text
BORDER     = HexColor('#CBD5E1')
CODE_BG    = HexColor('#F1F5F9')   # code block background
WARN_BG    = HexColor('#FEF3C7')   # callout background
SPEC_BG    = HexColor('#ECFDF5')   # specified bucket
IMPL_BG    = HexColor('#FEF3C7')   # impl-defined bucket
RES_BG     = HexColor('#FEE2E2')   # research bucket

# ---------------------------------------------------------------------------
# Page geometry
# ---------------------------------------------------------------------------
PAGE_W, PAGE_H = A4
MARGIN_L = 22 * mm
MARGIN_R = 22 * mm
MARGIN_T = 22 * mm
MARGIN_B = 22 * mm
CONTENT_W = PAGE_W - MARGIN_L - MARGIN_R

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------
styles = getSampleStyleSheet()

style_body = ParagraphStyle(
    'Body', parent=styles['Normal'],
    fontName='NotoSerifSC', fontSize=10.5, leading=15.5,
    textColor=PRIMARY, alignment=TA_JUSTIFY,
    spaceBefore=0, spaceAfter=6,
)

style_body_left = ParagraphStyle(
    'BodyLeft', parent=style_body,
    alignment=TA_LEFT,
)

style_lead = ParagraphStyle(
    'Lead', parent=style_body,
    fontSize=11.5, leading=17, textColor=PRIMARY,
    spaceAfter=10,
)

style_h1 = ParagraphStyle(
    'H1', parent=styles['Heading1'],
    fontName='NotoSerifSC-Bold', fontSize=18, leading=22,
    textColor=PRIMARY, spaceBefore=18, spaceAfter=10,
    keepWithNext=True,
)

style_h2 = ParagraphStyle(
    'H2', parent=styles['Heading2'],
    fontName='NotoSerifSC-Bold', fontSize=13.5, leading=18,
    textColor=ACCENT, spaceBefore=12, spaceAfter=6,
    keepWithNext=True,
)

style_h3 = ParagraphStyle(
    'H3', parent=styles['Heading3'],
    fontName='NotoSerifSC-Bold', fontSize=11.5, leading=15,
    textColor=PRIMARY, spaceBefore=8, spaceAfter=4,
    keepWithNext=True,
)

style_code = ParagraphStyle(
    'Code', parent=styles['Code'],
    fontName='DejaVuMono', fontSize=8.5, leading=12,
    textColor=PRIMARY, backColor=CODE_BG,
    leftIndent=8, rightIndent=8,
    borderPadding=6, spaceBefore=4, spaceAfter=8,
    alignment=TA_LEFT,
)

style_callout = ParagraphStyle(
    'Callout', parent=style_body,
    fontSize=10, leading=14, textColor=PRIMARY,
    backColor=WARN_BG, leftIndent=8, rightIndent=8,
    borderPadding=8, spaceBefore=6, spaceAfter=10,
)

style_caption = ParagraphStyle(
    'Caption', parent=style_body,
    fontSize=9, leading=12, textColor=MUTED,
    alignment=TA_LEFT, spaceBefore=2, spaceAfter=10,
)

style_table_h = ParagraphStyle(
    'TableHeader', parent=style_body,
    fontName='NotoSerifSC-Bold', fontSize=9.5, leading=12,
    textColor=white, alignment=TA_LEFT,
)

style_table_c = ParagraphStyle(
    'TableCell', parent=style_body,
    fontSize=9.5, leading=12.5, alignment=TA_LEFT,
    spaceBefore=0, spaceAfter=0,
)

style_table_mono = ParagraphStyle(
    'TableCellMono', parent=style_table_c,
    fontName='DejaVuMono', fontSize=8.5, leading=12,
)

# Cover styles
style_cover_kicker = ParagraphStyle(
    'CoverKicker', parent=style_body,
    fontName='DejaVuMono', fontSize=10, leading=14,
    textColor=ACCENT, alignment=TA_LEFT,
    spaceBefore=0, spaceAfter=8,
)

style_cover_title = ParagraphStyle(
    'CoverTitle', parent=style_body,
    fontName='NotoSerifSC-Bold', fontSize=34, leading=40,
    textColor=PRIMARY, alignment=TA_LEFT,
    spaceBefore=0, spaceAfter=12,
)

style_cover_sub = ParagraphStyle(
    'CoverSub', parent=style_body,
    fontName='NotoSerifSC-Light', fontSize=15, leading=22,
    textColor=MUTED, alignment=TA_LEFT,
    spaceBefore=0, spaceAfter=24,
)

style_cover_meta_k = ParagraphStyle(
    'CoverMetaK', parent=style_body,
    fontName='DejaVuMono', fontSize=8.5, leading=11,
    textColor=MUTED, alignment=TA_LEFT,
    spaceBefore=0, spaceAfter=2,
)

style_cover_meta_v = ParagraphStyle(
    'CoverMetaV', parent=style_body,
    fontName='NotoSerifSC-Bold', fontSize=11, leading=14,
    textColor=PRIMARY, alignment=TA_LEFT,
    spaceBefore=0, spaceAfter=10,
)

style_cover_summary_h = ParagraphStyle(
    'CoverSumH', parent=style_body,
    fontName='NotoSerifSC-Bold', fontSize=10, leading=13,
    textColor=ACCENT, alignment=TA_LEFT,
    spaceBefore=0, spaceAfter=4,
)

style_cover_summary_b = ParagraphStyle(
    'CoverSumB', parent=style_body,
    fontSize=9.5, leading=13, textColor=PRIMARY,
    alignment=TA_LEFT, spaceAfter=6,
)

# ---------------------------------------------------------------------------
# Helper flowables
# ---------------------------------------------------------------------------

class HRule(Flowable):
    """Horizontal rule with configurable color/weight."""
    def __init__(self, width=None, color=BORDER, thickness=0.5, space_before=4, space_after=4):
        Flowable.__init__(self)
        self.width = width
        self.color = color
        self.thickness = thickness
        self.space_before = space_before
        self.space_after = space_after
    def wrap(self, aw, ah):
        self.w = self.width or aw
        return (self.w, self.thickness + self.space_before + self.space_after)
    def draw(self):
        c = self.canv
        c.setStrokeColor(self.color)
        c.setLineWidth(self.thickness)
        y = self.space_after
        c.line(0, y, self.w, y)


def code_block(text):
    """Render a code block with monospace font and light background."""
    # Replace newlines with <br/> for ReportLab Paragraph
    safe = (text.replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('  ', '&nbsp;&nbsp;')
                .replace('\n', '<br/>'))
    return Paragraph(safe, style_code)


def callout(text, bg=WARN_BG, prefix='Note'):
    """Callout box with prefix label."""
    bg_hex = bg
    style = ParagraphStyle(
        'CalloutInline', parent=style_body,
        fontSize=10, leading=14, textColor=PRIMARY,
        backColor=bg_hex, leftIndent=8, rightIndent=8,
        borderPadding=8, spaceBefore=6, spaceAfter=10,
    )
    safe = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    return Paragraph(f'<b>{prefix}.</b> {safe}', style)


def styled_table(data, col_widths, header=True, zebra=True, header_bg=PRIMARY):
    """Build a styled table with header row and zebra striping."""
    rows = []
    for i, row in enumerate(data):
        new_row = []
        for cell in row:
            if isinstance(cell, str):
                if i == 0 and header:
                    new_row.append(Paragraph(cell, style_table_h))
                else:
                    new_row.append(Paragraph(cell, style_table_c))
            else:
                new_row.append(cell)
        rows.append(new_row)

    t = Table(rows, colWidths=col_widths, repeatRows=1 if header else 0)
    style_cmds = [
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('RIGHTPADDING', (0,0), (-1,-1), 6),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LINEBELOW', (0,0), (-1,-1), 0.25, BORDER),
        ('LINEABOVE', (0,0), (-1,0), 0.25, BORDER),
        ('LINEBEFORE', (0,0), (0,-1), 0.25, BORDER),
        ('LINEAFTER', (-1,0), (-1,-1), 0.25, BORDER),
    ]
    if header:
        style_cmds.append(('BACKGROUND', (0,0), (-1,0), header_bg))
        style_cmds.append(('BOTTOMPADDING', (0,0), (-1,0), 7))
        style_cmds.append(('TOPPADDING', (0,0), (-1,0), 7))
    if zebra and header:
        for i in range(1, len(data)):
            if i % 2 == 0:
                style_cmds.append(('BACKGROUND', (0,i), (-1,i), HexColor('#F8FAFC')))
    t.setStyle(TableStyle(style_cmds))
    return t


def bucket_table(rows):
    """Table for the 3-bucket discipline with color-coded rows."""
    data = [['Bucket', 'Meaning', 'Examples']]
    for r in rows:
        data.append(r)
    t = Table(data, colWidths=[28*mm, 60*mm, CONTENT_W - 28*mm - 60*mm], repeatRows=1)
    bg_colors = [None, SPEC_BG, IMPL_BG, RES_BG]
    style_cmds = [
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('RIGHTPADDING', (0,0), (-1,-1), 6),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('BACKGROUND', (0,0), (-1,0), PRIMARY),
        ('LINEBELOW', (0,0), (-1,-1), 0.25, BORDER),
        ('LINEBEFORE', (0,0), (0,-1), 0.25, BORDER),
        ('LINEAFTER', (-1,0), (-1,-1), 0.25, BORDER),
    ]
    for i, bg in enumerate(bg_colors):
        if bg is not None:
            style_cmds.append(('BACKGROUND', (0,i), (-1,i), bg))
    # Style each cell with appropriate paragraph
    rows_styled = []
    for i, row in enumerate(data):
        if i == 0:
            rows_styled.append([Paragraph(c, style_table_h) for c in row])
        else:
            cells = []
            for j, c in enumerate(row):
                if j == 0:
                    cells.append(Paragraph(f'<b>{c}</b>', style_table_c))
                else:
                    cells.append(Paragraph(c, style_table_c))
            rows_styled.append(cells)
    t = Table(rows_styled, colWidths=[28*mm, 60*mm, CONTENT_W - 28*mm - 60*mm], repeatRows=1)
    t.setStyle(TableStyle(style_cmds))
    return t


# ---------------------------------------------------------------------------
# Page templates
# ---------------------------------------------------------------------------

def cover_page(canvas, doc):
    """Cover page background — minimal, no chrome."""
    canvas.saveState()
    # Top accent bar
    canvas.setFillColor(ACCENT)
    canvas.rect(0, PAGE_H - 6*mm, PAGE_W, 6*mm, stroke=0, fill=1)
    # Bottom subtle line
    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN_L, 20*mm, PAGE_W - MARGIN_R, 20*mm)
    # Footer text
    canvas.setFont('DejaVuMono', 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(MARGIN_L, 14*mm, 'POND / RFC 1 / ARCHITECTURE-FROZEN / 2026-07')
    canvas.drawRightString(PAGE_W - MARGIN_R, 14*mm, 'v 1.0')
    canvas.restoreState()


def body_page(canvas, doc):
    """Body page header/footer."""
    canvas.saveState()
    # Header rule
    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.25)
    canvas.line(MARGIN_L, PAGE_H - MARGIN_T + 8*mm,
                PAGE_W - MARGIN_R, PAGE_H - MARGIN_T + 8*mm)
    # Header text
    canvas.setFont('DejaVuMono', 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(MARGIN_L, PAGE_H - MARGIN_T + 11*mm,
                      'POND RFC 1 — STORAGE & VERSIONED STATE')
    canvas.drawRightString(PAGE_W - MARGIN_R, PAGE_H - MARGIN_T + 11*mm,
                           'v 1.0 / ARCHITECTURE-FROZEN')
    # Footer
    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.25)
    canvas.line(MARGIN_L, MARGIN_B - 6*mm,
                PAGE_W - MARGIN_R, MARGIN_B - 6*mm)
    canvas.setFont('DejaVuMono', 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(MARGIN_L, MARGIN_B - 10*mm, 'Pond / Capability-Oriented Data Runtime')
    canvas.drawRightString(PAGE_W - MARGIN_R, MARGIN_B - 10*mm,
                           f'Page {doc.page}')
    canvas.restoreState()


# ---------------------------------------------------------------------------
# Content builders
# ---------------------------------------------------------------------------

def build_cover():
    """Build the cover page."""
    story = []
    # Top spacer to push down past accent bar
    story.append(Spacer(1, 40*mm))

    story.append(Paragraph('RFC 1 / ARCHITECTURE-FROZEN', style_cover_kicker))
    story.append(Paragraph('Pond', style_cover_title))
    story.append(Paragraph('Storage &amp; Versioned State', style_cover_sub))

    story.append(HRule(color=ACCENT, thickness=1.5, space_before=2, space_after=14))

    # Meta grid
    meta_data = [
        ('DOCUMENT',     'Pond RFC 1 — Storage & Versioned State'),
        ('VERSION',      '1.0'),
        ('STATUS',       'Architecture-Frozen'),
        ('DATE',         'July 2026'),
        ('AUDIENCE',     'Implementers, contributors, systems architects'),
        ('PHILOSOPHY',   'One copy. Infinite execution. Zero coordination unless necessary.'),
    ]
    meta_rows = []
    for k, v in meta_data:
        meta_rows.append([
            Paragraph(k, style_cover_meta_k),
            Paragraph(v, style_cover_meta_v),
        ])
    meta_table = Table(meta_rows, colWidths=[35*mm, CONTENT_W - 35*mm])
    meta_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
    ]))
    story.append(meta_table)

    story.append(Spacer(1, 14*mm))

    # Summary block
    story.append(HRule(color=BORDER, thickness=0.5, space_before=0, space_after=8))
    story.append(Paragraph('SUMMARY', style_cover_summary_h))
    story.append(Paragraph(
        'This document formally specifies the Pond storage ABI, object lifecycle, '
        'and Versioned State &mdash; the fundamental primitive from which transactions, '
        'snapshots, branches, time travel, replication, and placement all derive. '
        'It is the first of five RFCs that together specify the Pond architecture '
        'in implementable form.',
        style_cover_summary_b))
    story.append(Paragraph(
        'Every claim in this document belongs to one of three buckets: '
        '<b>Specified</b> (architectural, implementations must conform), '
        '<b>Implementation-defined</b> (architecture permits multiple approaches), or '
        '<b>Research</b> (acknowledged unsolved, future work). This discipline &mdash; '
        'borrowed from LLVM, Linux, Raft, TigerBeetle, and FoundationDB &mdash; '
        'is what makes the specification honest and implementable.',
        style_cover_summary_b))

    story.append(PageBreak())
    return story


def section_1_introduction():
    story = []
    story.append(Paragraph('1. Introduction &amp; Philosophy', style_h1))

    story.append(Paragraph(
        'Pond is a capability-oriented data runtime: a minimal core coordinating many '
        'execution strategies over a single versioned, immutable, one-copy object '
        'substrate, with everything staying local unless coordination is provably '
        'necessary. The architecture is not a clone of Spark, Flink, Databricks, or '
        'Iceberg. It is a distinct architectural direction whose organizing principle '
        'is the composition of execution capabilities over Versioned State.',
        style_lead))

    story.append(Paragraph('1.1 Philosophy', style_h2))
    story.append(Paragraph(
        '<b>One copy. Infinite execution. Zero coordination unless necessary.</b>',
        style_callout))
    story.append(Paragraph(
        'These three clauses capture the discipline that governs every architectural '
        'decision in Pond. <i>One copy</i> means that exactly one artifact &mdash; '
        'immutable sealed objects in object storage &mdash; is canonical; everything '
        'else (catalogs, indexes, materialized views, the OPEN object log) is a '
        'cache rebuildable in bounded time. <i>Infinite execution</i> means that '
        'capabilities compose without limit: SQL, streaming, transactions, AI, vector, '
        'graph, and future workloads all run over the same substrate through '
        'declarative capability interfaces. <i>Zero coordination unless necessary</i> '
        'means that everything stays local &mdash; single-node, single-process, '
        'in-process &mdash; unless the system can prove that coordination across '
        'nodes is required for correctness. This is the discipline behind SQLite, '
        'Git, TigerBeetle, DuckDB, and every other system that became successful '
        'by remaining small.',
        style_body))

    story.append(Paragraph('1.2 The Seven Architectural Laws', style_h2))
    story.append(Paragraph(
        'The architecture is governed by seven non-negotiable laws. Every change '
        'to the architecture must strengthen at least one of them; any change that '
        'weakens one is rejected by default. The laws are listed here for reference; '
        'their full implications are woven through every section of this RFC and '
        'the four RFCs that follow.',
        style_body))

    laws_data = [
        ['#', 'Law', 'What it forbids'],
        ['1', 'One canonical copy. Everything else is cache.',
         'Any second durable representation (LTAP\'s Postgres+Parquet; Fluss\'s log+lake).'],
        ['2', 'Storage is permanent. Execution is replaceable.',
         'Baking any execution strategy into storage; making any backend uncircumventable.'],
        ['3', 'The core must remain tiny.',
         'Adding subsystems that do not replace existing ones; concept accretion.'],
        ['4', 'Distribution is an optimization, not another architecture.',
         '"Cluster mode" vs "embedded mode" as separate code paths.'],
        ['5', 'Capabilities before products.',
         'Hardcoding Kafka, GPU, vectors, AI, formats, or replication algorithms.'],
        ['6', 'Mechanical sympathy &mdash; measurable.',
         'Abstractions that hide hardware cost; slogans without budgets.'],
        ['7', 'The complexity budget.',
         'Feature creep; subsystem accretion; changes that do not remove more than they add.'],
    ]
    story.append(styled_table(laws_data,
                              col_widths=[10*mm, 70*mm, CONTENT_W - 80*mm]))
    story.append(Paragraph(
        'Table 1.1: The seven architectural laws. Every RFC and every PR must answer '
        'which law it strengthens and none that it weakens.',
        style_caption))

    story.append(Paragraph('1.3 Scope of this RFC', style_h2))
    story.append(Paragraph(
        'This is RFC 1 of 5. It specifies the storage layer: the four storage '
        'syscalls that form the internal kernel ABI, the object lifecycle state '
        'machine, Versioned State as the fundamental primitive, object layout '
        'targets, versioning and compatibility rules, the storage-relevant subset '
        'of failure domains, chaos tests as architectural acceptance tests, the '
        'storage-layer invariants, performance budgets, and the dependency budget '
        'for the storage kernel. The other four RFCs cover the Planner and IR '
        '(RFC 2), the transaction and consistency model (RFC 3), the capability '
        'and placement model (RFC 4), and the operational architecture (RFC 5).',
        style_body))

    story.append(Paragraph(
        'The reader of this document is expected to be a systems engineer '
        'comfortable with content-addressed storage (Git), log-structured '
        'systems (FoundationDB, Postgres WAL), and capability or plugin '
        'architectures (LLVM backends, Linux device drivers). No knowledge of '
        'DuckDB, Spark, or any specific lakehouse format is assumed, though '
        'familiarity with Apache Iceberg or Delta Lake will make the '
        'Versioned State section easier to read.',
        style_body))
    return story


def section_2_buckets():
    story = []
    story.append(Paragraph('2. The 3-Bucket Discipline', style_h1))

    story.append(Paragraph(
        'Every claim in this RFC &mdash; and in every subsequent RFC &mdash; '
        'belongs to exactly one of three buckets. The discipline is borrowed '
        'from how mature systems specifications are written: LLVM separates '
        'the IR specification from backend-specific behavior; Linux separates '
        'the syscall ABI from driver implementation; Raft separates the '
        'consensus algorithm from log-structured storage; TigerBeetle separates '
        'the deterministic state machine from the replication protocol; '
        'FoundationDB separates the transaction protocol from storage-server '
        'implementation. Pond follows the same discipline.',
        style_body))

    story.append(bucket_table([
        ['Specified',
         'Architectural. Implementations must conform. Two independent '
         'implementations of a Specified behavior must produce identical '
         'observable results.',
         'The four storage syscalls. The object lifecycle state machine. '
         'The content-addressed DAG pattern. Versioning and compatibility '
         'rules. The storage-layer invariants.'],
        ['Implementation-defined',
         'The architecture permits multiple approaches. The implementation '
         'chooses one and documents it. Different implementations may make '
         'different choices without violating the architecture.',
         'HLC vs TrueTime for timestamps. Raft vs storage-aware replication. '
         'Object size within 128&nbsp;MB&ndash;1&nbsp;GB. Root store backend '
         '(embedded Raft, FDB, etcd, Postgres).'],
        ['Research',
         'Acknowledged unsolved. Future work. Named honestly rather than '
         'pretended solved. Multi-year effort, like LLVM IR was.',
         'Streaming semantics (exactly-once, watermarks, incremental MVs). '
         'Distributed execution (Exchange across nodes). Cross-backend '
         'cost-based planning. Storage-aware replication protocol.'],
    ]))
    story.append(Paragraph(
        'Table 2.1: The 3-bucket discipline. Every claim in this RFC is in '
        'one of these buckets.',
        style_caption))

    story.append(Paragraph('2.1 Why the discipline matters', style_h2))
    story.append(Paragraph(
        'Architectures die when they quietly assume solutions to research '
        'problems. Spark assumed a unified RDD abstraction would handle '
        'streaming &mdash; and ended up adding Structured Streaming as a '
        'separate subsystem. Databricks LTAP assumes sub-second Postgres-to-'
        'Iceberg sync &mdash; measured sync lag in production is minutes, '
        'not milliseconds. Materialize spent years honestly acknowledging '
        'that differential dataflow was the hard research problem. The '
        '3-bucket discipline forces the architect to say, explicitly, '
        '"this is specified," "this is impl-defined," or "this is research." '
        'It prevents the quiet assumption that destroys architectures years '
        'later.',
        style_body))

    story.append(Paragraph('2.2 How to read this RFC', style_h2))
    story.append(Paragraph(
        'When this RFC says a behavior is <b>Specified</b>, two independent '
        'implementations must produce identical observable results, or one '
        'of them is non-conformant. When it says a behavior is '
        '<b>Implementation-defined</b>, the implementation must document its '
        'choice and the trade-offs it implies. When it says a behavior is '
        '<b>Research</b>, the architecture permits a future solution but '
        'does not claim one exists today. Implementers who ignore this '
        'distinction and pretend a Research item is Specified will produce '
        'systems that fail in production for reasons the architecture '
        'already acknowledged.',
        style_body))
    return story


def section_3_syscalls():
    story = []
    story.append(Paragraph('3. The Storage ABI &mdash; Four Syscalls', style_h1))

    story.append(Paragraph(
        'The Pond storage kernel exposes exactly four operations. These are '
        'the internal kernel ABI &mdash; the equivalent of Linux syscalls. '
        'They are not the public SQL API. Users of Pond see CREATE TABLE, '
        'INSERT, SELECT, CREATE PIPESOURCE, CREATE INDEX, CREATE BRANCH. '
        'The four syscalls are the layer below SQL: every user-facing '
        'operation compiles down to a graph of these four primitives, '
        'executed by the Coordinator against the storage kernel. The '
        'surface is irreducible: fewer than four primitives cannot express '
        'a named, content-addressed, immutable database. More than four '
        'would either be library code (Collect is graph traversal via '
        'Read+Reference) or leak execution concepts into storage.',
        style_body))

    story.append(Paragraph('3.1 The four operations', style_h2))

    story.append(Paragraph('Read', style_h3))
    story.append(code_block(
        'Read(hash_or_name) -> Bytes\n'
        '\n'
        '// If given a content hash (e.g. sha256:abc123...), return the\n'
        '// bytes of the sealed object with that hash.\n'
        '// If given a name (e.g. "lake.events"), resolve the name to\n'
        '// its current commit hash via the root pointer namespace,\n'
        '// then return the bytes of the object at that hash.\n'
        '\n'
        '// Precondition: hash exists OR name is bound in the root\n'
        '//   pointer namespace of the current database.\n'
        '// Postcondition: returned bytes match hash(hash) exactly,\n'
        '//   or raise NOT_FOUND.\n'
        '// Consistency: linearizable for name resolution (a name\n'
        '//   always resolves to its most-recently-committed hash);\n'
        '//   content-addressed reads are by construction consistent\n'
        '//   (a hash is a fixed value).'
    ))

    story.append(Paragraph('Write', style_h3))
    story.append(code_block(
        'Write(bytes) -> FragmentHandle\n'
        '\n'
        '// Append a fragment (a batch of rows encoded as Arrow IPC,\n'
        '// or a format-plugin-defined OPEN format) to the OPEN object\n'
        '// of the current transaction on the current shard.\n'
        '// Returns a FragmentHandle that includes the open_hash, the\n'
        '// fragment offset, and the LSN (log sequence number) at which\n'
        '// the fragment was replicated.\n'
        '\n'
        '// Precondition: an OPEN object exists for the current\n'
        '//   transaction on the current shard. If not, the runtime\n'
        '//   creates one implicitly on the first Write.\n'
        '// Postcondition: the fragment is fsync\'d to the Raft log\n'
        '//   of the shard before the call returns. The fragment is\n'
        '//   visible to subsequent Reads of the OPEN object.\n'
        '// Consistency: linearizable. A Write is acknowledged only\n'
        '//   after Raft quorum fsync.'
    ))

    story.append(Paragraph('Seal', style_h3))
    story.append(code_block(
        'Seal(open_hash) -> SealedHash\n'
        '\n'
        '// Convert the OPEN object identified by open_hash into a\n'
        '// SEALED object. The OPEN object\'s Arrow IPC (or other OPEN\n'
        '// format) byte stream is converted to a Parquet file on\n'
        '// object storage, the final content hash is computed, the\n'
        '// metadata object (stats, bloom, pk_range, lineage, schema_id,\n'
        '// branch_id, txid, ttl, checksums) is written, and the object\n'
        '// is marked SEALED.\n'
        '\n'
        '// Precondition: open_hash refers to an OPEN object whose\n'
        '//   replication is complete (all fragments acked by Raft\n'
        '//   quorum).\n'
        '// Postcondition: the OPEN object is no longer writable. The\n'
        '//   returned SealedHash is the content-addressed hash of the\n'
        '//   sealed Parquet bytes plus the metadata object. The OPEN\n'
        '//   object\'s Raft log prefix up to the seal point is\n'
        '//   eligible for truncation (it is now derivable from the\n'
        '//   sealed Parquet).\n'
        '// Consistency: idempotent. Sealing an already-sealed object\n'
        '//   returns the same hash.'
    ))

    story.append(Paragraph('Reference', style_h3))
    story.append(code_block(
        'Reference(name, hash) -> ()\n'
        '\n'
        '// Set a mutable name -> hash mapping in the root pointer\n'
        '// namespace. This is the only mutable operation in the storage\n'
        '// kernel. Names are scoped to a database; hashes are global\n'
        '// (content-addressed).\n'
        '\n'
        '// Precondition: hash exists (the referenced object has been\n'
        '//   sealed). The caller has write permission on the name\'s\n'
        '//   namespace.\n'
        '// Postcondition: subsequent Read(name) returns the bytes of\n'
        '//   the object at hash. The previous binding (if any) is\n'
        '//   preserved in the commit DAG as the parent of this\n'
        '//   reference update.\n'
        '// Consistency: linearizable. A Reference update is\n'
        '//   acknowledged only after Raft quorum fsync on the root\n'
        '//   pointer store.'
    ))

    story.append(Paragraph('3.2 What was eliminated: Collect', style_h2))
    story.append(Paragraph(
        'An earlier draft of this ABI included a fifth operation, '
        '<code>Collect(predicates)</code>, which listed hashes matching '
        'given predicates. It has been eliminated. Collect is graph '
        'traversal &mdash; it is implementable as a library function '
        'over Read+Reference (walk the commit DAG, filter by predicate). '
        'Git has no Collect syscall; <code>git log</code> walks the DAG. '
        'Making Collect primitive would have leaked query concepts into '
        'storage. The four operations above are the irreducible minimum '
        'for a named, content-addressed, immutable database: Read to '
        'access data, Write to create data, Seal to make data immutable, '
        'Reference to name data.',
        style_body))

    story.append(Paragraph('3.3 Failure modes (Specified)', style_h2))
    story.append(Paragraph(
        'Each syscall has specified failure semantics. The implementation '
        'chooses how to detect and recover; the semantics are architectural.',
        style_body))

    failure_data = [
        ['Failure', 'Read', 'Write', 'Seal', 'Reference'],
        ['Network partition\n(leader isolated)',
         'Stale reads from followers; linearizable reads block',
         'Blocks on majority side; unavailable on minority',
         'Blocks (requires quorum)',
         'Blocks (requires quorum)'],
        ['Disk full\n(local NVMe)',
         'Continues from S3',
         'Blocks; backpressure to client',
         'Blocks',
         'Blocks'],
        ['S3 unavailable',
         'Continues from NVMe cache (bounded)',
         'Continues (OPEN objects stay on NVMe)',
         'Blocks (cannot write Parquet)',
         'Continues (root store is local/Raft)'],
        ['Quorum lost',
         'Stale reads from S3',
         'Unavailable for that shard',
         'Unavailable for that shard',
         'Unavailable for that namespace'],
        ['Object not found',
         'Raises NOT_FOUND (deterministic)',
         'n/a',
         'Raises INVALID_OPEN_HASH',
         'Raises HASH_NOT_FOUND'],
    ]
    story.append(styled_table(failure_data,
        col_widths=[35*mm, 32*mm, 32*mm, 32*mm, CONTENT_W - 35*mm - 96*mm]))
    story.append(Paragraph(
        'Table 3.1: Failure semantics of each syscall. The semantics are '
        'architectural; recovery protocols are impl-defined.',
        style_caption))

    story.append(Paragraph('3.4 Consistency guarantees (Specified)', style_h2))
    story.append(Paragraph(
        '<b>Linearizable writes.</b> Write, Seal, and Reference are '
        'acknowledged only after Raft quorum fsync on the relevant shard. '
        'A successful write is durable across quorum failures. An '
        'unacknowledged write may or may not be durable; the client must '
        'retry on timeout.',
        style_body))
    story.append(Paragraph(
        '<b>Snapshot reads.</b> Read at a content hash is by construction '
        'consistent &mdash; the hash refers to fixed bytes that never '
        'change. Read at a name resolves the name to its current commit '
        'hash via the root pointer namespace, then reads at that hash. '
        'The snapshot is the commit hash; once resolved, the read is '
        'immune to concurrent commits.',
        style_body))
    story.append(Paragraph(
        '<b>Cross-region reads.</b> Async Raft followers in remote regions '
        'serve reads at their last-applied commit hash. Read-your-writes '
        'within the primary region; eventually consistent (bounded by '
        'replication lag) across regions. This is the FoundationDB / '
        'Spanner / TigerBeetle pattern &mdash; the proven trade for '
        'single-region linearizability without specialized hardware.',
        style_body))
    return story


def section_4_lifecycle():
    story = []
    story.append(Paragraph('4. Object Lifecycle State Machine', style_h1))

    story.append(Paragraph(
        'Every object in Pond transitions through a fixed lifecycle. The '
        'state machine is Specified: the states, the allowed transitions, '
        'and the invariants per state are architectural. The triggers '
        'for transitions (when to seal, when to compact, when to archive, '
        'when to gc) are Implementation-defined: the implementation '
        'chooses policies, within the constraints of the invariants.',
        style_body))

    story.append(Paragraph('4.1 The state machine', style_h2))
    story.append(code_block(
        '                 Write (append fragment)\n'
        '                       |\n'
        '                       v\n'
        '                   +--------+\n'
        '                   |  OPEN  |  mutable, appendable, Raft-replicated\n'
        '                   +--------+\n'
        '                       |\n'
        '                       | Seal (Arrow IPC -> Parquet on S3)\n'
        '                       v\n'
        '                  +---------+\n'
        '                  | SEALED  |  immutable, content-addressed, hash=hash(bytes)\n'
        '                  +---------+\n'
        '                       |\n'
        '                       | Compact (merge N sealed of same table -> 1 sealed)\n'
        '                       v\n'
        '                 +-----------+\n'
        '                 | COMPACTED |  immutable, larger, fewer objects\n'
        '                 +-----------+\n'
        '                       |\n'
        '                       | Archive (move to cold storage tier)\n'
        '                       v\n'
        '                 +-----------+\n'
        '                 | ARCHIVED  |  immutable, cold storage class\n'
        '                 +-----------+\n'
        '                       |\n'
        '                       | GC (after TTL / no references)\n'
        '                       v\n'
        '                    +----+\n'
        '                    |GONE|  removed\n'
        '                    +----+'
    ))

    story.append(Paragraph('4.2 Invariants per state (Specified)', style_h2))

    inv_data = [
        ['State', 'Mutability', 'Replication', 'Invariants'],
        ['OPEN',
         'Mutable. Appends allowed via Write.',
         'Raft-replicated fragment log on NVMe. Bounded by drain lag.',
         'Fsync\'d to Raft quorum before Write ack. Rebuildable from sealed '
         'objects + log tail in O(un-sealed ops). Truncated after Seal.'],
        ['SEALED',
         'Immutable. No writes allowed.',
         'Object storage (S3/GCS/Azure). Content-addressed.',
         'Hash = hash(bytes), forever. Readable by any future Pond version. '
         'Drop and re-fetch from S3 if local cache lost.'],
        ['COMPACTED',
         'Immutable. Same content, different layout.',
         'Object storage.',
         'Logically equivalent to source sealed objects (same rows, possibly '
         'different physical layout). Lineage preserved in metadata.'],
        ['ARCHIVED',
         'Immutable. Moved to cold tier.',
         'Cold storage class (S3 Glacier, etc.).',
         'Readable but slower (minutes-hours latency). Restorable to warm tier '
         'on demand.'],
        ['GONE',
         'Removed.',
         'n/a.',
         'All references removed. Hash is dead. Cannot be resurrected.'],
    ]
    story.append(styled_table(inv_data,
        col_widths=[22*mm, 35*mm, 40*mm, CONTENT_W - 97*mm]))
    story.append(Paragraph(
        'Table 4.1: Invariants per lifecycle state. The state machine is '
        'architectural; transition triggers are impl-defined.',
        style_caption))

    story.append(Paragraph('4.3 Transition triggers (Implementation-defined)', style_h2))
    story.append(Paragraph(
        'The implementation chooses when to trigger each transition, '
        'within the constraints of the invariants. Reasonable defaults:',
        style_body))
    story.append(Paragraph(
        '<b>Seal:</b> when the OPEN object exceeds a size threshold '
        '(default 512&nbsp;MB) or an age threshold (default 60 seconds), '
        'or when the transaction commits. Seal is also triggered by '
        'backpressure when the Raft log exceeds a configurable depth.',
        style_body))
    story.append(Paragraph(
        '<b>Compact:</b> when the count of small sealed objects for a '
        'table exceeds a threshold (default 64). Bin-packing compaction '
        'is the v1 default; tiered compaction (RocksDB-style) is a '
        'research target.',
        style_body))
    story.append(Paragraph(
        '<b>Archive:</b> when an object has not been read in N days '
        '(default 30). Driven by the placement capability, not the '
        'storage kernel itself.',
        style_body))
    story.append(Paragraph(
        '<b>GC:</b> when an object has no live references and its TTL '
        'has expired (default 90 days after last reference dropped). '
        'GC is conservative: an object is only removed when the root '
        'pointer namespace, all branches, and all MV lineage commits '
        'agree it is unreferenced.',
        style_body))
    return story


def section_5_versioned_state():
    story = []
    story.append(Paragraph('5. Versioned State &mdash; The Fundamental Primitive', style_h1))

    story.append(Paragraph(
        'Versioned State is the fundamental primitive of Pond. Everything '
        'else &mdash; transactions, snapshots, branches, time travel, '
        'replication, placement &mdash; is a derived semantic or policy '
        'over Versioned State. Earlier drafts of the architecture had '
        'transactions and placement as separate permanent concepts; '
        'elevating Versioned State and demoting them to derived semantics '
        'was the conceptual move that made the architecture coherent.',
        style_lead))

    story.append(Paragraph('5.1 Definition', style_h2))
    story.append(Paragraph(
        'Versioned State is the union of three things: (1) the content-'
        'addressed DAG of immutable objects, (2) the object lifecycle '
        'state machine defined in section 4, and (3) the root pointer '
        'namespace that names versions. From these three, every other '
        'versioning concept derives:',
        style_body))

    deriv_data = [
        ['Concept', 'How it derives from Versioned State'],
        ['Snapshot',
         'A Read at a specific commit hash. The hash refers to immutable '
         'bytes; the snapshot is by construction consistent.'],
        ['Branch',
         'A named pointer to a commit hash. Creating a branch is a '
         'Reference operation; the branch shares all sealed objects with '
         'its parent (copy-on-write semantics).'],
        ['Tag',
         'A named pointer to a commit hash, identical to a branch but '
         'conventionally immutable. Implemented as a Reference.'],
        ['Time travel',
         'A Read at a past commit hash. The commit DAG preserves all '
         'historical hashes; reading at any of them returns the state '
         'at that point.'],
        ['Transaction',
         'A sequence of Write/Seal/Reference operations bounded by '
         'BEGIN and COMMIT. The COMMIT creates a new commit object '
         'whose parent is the previous commit. Specified in RFC 3.'],
        ['Replication',
         'A policy over Versioned State: which sealed objects are '
         'replicated where, and how the root pointer namespace is '
         'kept consistent across regions.'],
        ['Placement',
         'A policy over Versioned State: which sealed objects live on '
         'which storage tier (NVMe, S3 Standard, Glacier) and which '
         'region. Specified in RFC 4.'],
        ['Rollback',
         'A Reference operation that points a name to a previous '
         'commit hash. No data is destroyed; the current commit simply '
         'becomes unreferenced (and eventually GC\'d).'],
    ]
    story.append(styled_table(deriv_data,
        col_widths=[28*mm, CONTENT_W - 28*mm]))
    story.append(Paragraph(
        'Table 5.1: How every versioning concept derives from Versioned State.',
        style_caption))

    story.append(Paragraph('5.2 The content-addressed DAG (Specified)', style_h2))
    story.append(Paragraph(
        'The DAG follows the Git model. There are four object types. '
        'Each is content-addressed: the hash of an object is the hash of '
        'its serialized bytes. The four types are:',
        style_body))

    dag_data = [
        ['Type', 'What it holds', 'Analogy'],
        ['blob',
         'Raw bytes &mdash; a Parquet file, an Arrow fragment, an index, '
         'a stats block. Opaque to the DAG.',
         'Git blob'],
        ['tree',
         'A directory mapping <code>{name -&gt; hash}</code>. A table '
         'is a tree of blob refs. A schema is a tree of column defs. '
         'An index is a tree of <code>(key -&gt; blob+offset)</code> refs.',
         'Git tree'],
        ['commit',
         'A snapshot pointer: <code>{tree_hash, parent_commit_hash, '
         'author, timestamp, message}</code>. A table at a point in time '
         'is a commit.',
         'Git commit'],
        ['tag',
         'A named pointer to a commit: <code>main</code>, <code>v2</code>, '
         '<code>agent_exp_42</code>, <code>events_per_min_mv</code>. '
         'Branches, versions, MVs, named snapshots are all tags.',
         'Git tag'],
    ]
    story.append(styled_table(dag_data,
        col_widths=[18*mm, CONTENT_W - 50*mm, 32*mm]))
    story.append(Paragraph(
        'Table 5.2: The four object types. Pattern over Read/Write/Seal/'
        'Reference, not separate syscalls.',
        style_caption))

    story.append(Paragraph(
        'These four types are <i>patterns over the four syscalls</i>, not '
        'separate primitives. A blob is <code>Seal(Write(bytes))</code>. '
        'A tree is <code>Seal(Write(serialized_references))</code>. A '
        'commit is <code>Seal(Write(tree_hash + parent_hash + metadata))'
        '</code>. A tag is <code>Reference(name, commit_hash)</code>. '
        'The DAG is a convention for organizing objects; the storage '
        'kernel does not know about blob/tree/commit/tag as distinct '
        'types. This is the same discipline as Git, where the object '
        'model is a convention layered on top of a content-addressed '
        'blob store.',
        style_body))

    story.append(Paragraph('5.3 The root pointer namespace (Specified)', style_h2))
    story.append(Paragraph(
        'The root pointer namespace is the only mutable metadata in '
        'Pond. It maps names to commit hashes: <code>lake.events -&gt; '
        'commit_abc123</code>, <code>lake.users -&gt; commit_def456</code>, '
        '<code>branch:exp -&gt; commit_ghi789</code>. Without it, you '
        'would need to know hashes to read anything &mdash; that is IPFS, '
        'not a database. Databases need names.',
        style_body))

    story.append(Paragraph(
        'The root pointer namespace is small, mutable, and strongly '
        'consistent. It is replicated via its own Raft group (or, '
        'alternatively, backed by an external KV store like FoundationDB '
        'or etcd &mdash; impl-defined). Every other piece of metadata '
        '&mdash; commits, trees, schemas, indexes, stats, permissions '
        '&mdash; is content-addressed and immutable, stored as sealed '
        'objects on S3.',
        style_body))

    story.append(Paragraph('5.4 The metadata model (Specified)', style_h2))

    meta_arch_data = [
        ['Layer', 'What it stores', 'Where', 'Consistency'],
        ['Root pointers',
         'name -&gt; commit_hash (small, KB-MB per database)',
         'Root Store (Raft or external KV)',
         'Linearizable'],
        ['Commits',
         'snapshot metadata (tree hash, parent, timestamp)',
         'S3 (immutable, content-addressed)',
         'Content-addressed'],
        ['Trees',
         'file/index references',
         'S3 (immutable, content-addressed)',
         'Content-addressed'],
        ['Schemas',
         'column definitions, types',
         'S3 (immutable, content-addressed)',
         'Content-addressed'],
        ['Indexes',
         'key -&gt; blob+offset mappings',
         'S3 (immutable, content-addressed)',
         'Content-addressed'],
        ['Stats',
         'per-file column min/max/bloom',
         'S3 (immutable, content-addressed)',
         'Content-addressed'],
        ['Permissions',
         'ACL trees (principal -&gt; capability)',
         'S3 (immutable, content-addressed)',
         'Content-addressed'],
    ]
    story.append(styled_table(meta_arch_data,
        col_widths=[28*mm, 50*mm, 45*mm, CONTENT_W - 123*mm]))
    story.append(Paragraph(
        'Table 5.3: The metadata stack. Only root pointers are mutable; '
        'everything else is content-addressed.',
        style_caption))

    story.append(Paragraph(
        'This is why the architecture scales: only the root pointer layer '
        'requires coordination, and it is tiny. Everything else is '
        'content-addressed and naturally distributed &mdash; any node can '
        'cache any sealed object, any node can serve any read, and '
        'consistency is by construction (a hash is a fixed value). This '
        'is the same pattern as Git (most refs are content-addressed; '
        'HEAD is a tiny text file), Nix (store paths are content hashes; '
        'the manifest is small), and OCI (image layers are content-'
        'addressed; the manifest is a small JSON document).',
        style_body))

    story.append(Paragraph('5.5 The recursion proof', style_h2))
    story.append(Paragraph(
        'The catalog itself is a DuckLake-style table on S3 &mdash; its '
        'rows are metadata, its hot portion is cached in DuckDB memory, '
        'its cold portion is Parquet on S3. The catalog\'s own metadata '
        '(the catalog\'s catalog) is tiny and lives in the root pointer '
        'namespace. Recursion terminates cleanly at the root store. '
        'There is no infinite regress: at the bottom of the stack is a '
        'small strongly-consistent namespace, and above it is layered '
        'content-addressed immutable metadata.',
        style_body))

    story.append(Paragraph('5.6 State Views (future RFC)', style_h2))
    story.append(Paragraph(
        'The reader will notice that snapshot, branch, tag, time travel, '
        'rollback, and read-timestamp are all "views" over Versioned '
        'State. A future RFC may formalize <i>State Views</i> as a '
        'unified abstraction: a declarative way to specify a view '
        '(snapshot at hash H, branch B as of time T, etc.) that the '
        'planner can reason about. This is not specified in RFC 1; it '
        'is flagged here as a likely RFC 2 or RFC 5 topic. The current '
        'specification handles each view type as a derived semantic '
        'over Versioned State (per Table 5.1), which is sufficient for '
        'implementation.',
        style_body))
    return story


def section_6_layout():
    story = []
    story.append(Paragraph('6. Object Layout (Implementation-Defined)', style_h1))

    story.append(Paragraph(
        'Object layout &mdash; the physical size and structure of sealed '
        'Parquet files &mdash; is Implementation-defined. The architecture '
        'permits a range of choices; the implementation chooses within '
        'that range and documents its defaults. The targets below are '
        'the v1 defaults; they are subject to change based on '
        'benchmark results. They are not architectural guarantees.',
        style_body))

    layout_data = [
        ['Parameter', 'v1 default', 'Range', 'Trade-off'],
        ['Sealed object size', '512 MB',
         '128 MB &ndash; 1 GB',
         '< 128 MB: small-files problem (S3 LIST cost, metadata explosion). '
         '&gt; 1 GB: scan skew, memory pressure, compaction cost.'],
        ['Row group size', '128 MB',
         '64 MB &ndash; 256 MB',
         'Parquet default. Larger groups improve sequential scan; smaller '
         'groups improve point-lookup skip.'],
        ['Column chunk size', '8&ndash;16 MB',
         '4&ndash;32 MB',
         'Affects memory pressure during scan; must fit comfortably in L2.'],
        ['Compression', 'ZSTD level 3',
         'ZSTD 1-19, LZ4, Snappy',
         'ZSTD-3 is the Pareto-optimal point for analytical workloads. '
         'Higher levels improve ratio but cost CPU.'],
        ['Sparse index granularity',
         '1 entry per 1M rows',
         '1 per 100K &ndash; 10M rows',
         'Smaller granularities speed point lookups but inflate the index. '
         '~1 KB per million rows is the ClickHouse default.'],
    ]
    story.append(styled_table(layout_data,
        col_widths=[40*mm, 25*mm, 30*mm, CONTENT_W - 95*mm]))
    story.append(Paragraph(
        'Table 6.1: Object layout parameters (impl-defined).',
        style_caption))

    story.append(Paragraph('6.1 Why these ranges', style_h2))
    story.append(Paragraph(
        'The 128&nbsp;MB&ndash;1&nbsp;GB range for sealed object size is '
        'not arbitrary. Iceberg targets 512&nbsp;MB; Delta targets 1&nbsp;GB; '
        'Paimon targets 256&nbsp;MB. Pond defaults to 512&nbsp;MB (the '
        'Iceberg default, which has the most production validation) and '
        'is configurable per table. Tables with high write throughput '
        'may benefit from smaller sealed objects (faster seal cycles); '
        'tables with heavy analytical scan workloads may benefit from '
        'larger ones (fewer metadata entries, better sequential IO).',
        style_body))

    story.append(Paragraph('6.2 Format neutrality (Specified)', style_h2))
    story.append(Paragraph(
        'No storage format is privileged. Parquet is the v1 default for '
        'sealed objects; Arrow IPC is the v1 default for OPEN objects. '
        'Both are pluggable via the <code>formats/</code> plugin taxonomy. '
        'A future implementation could use Lance (for ML workloads), '
        'Vortex (for newer columnar compression), or a custom format '
        'without changing the storage kernel. The kernel only requires '
        'that sealed objects are content-addressed and immutable &mdash; '
        'the format is the plugin\'s concern.',
        style_body))

    story.append(Paragraph('6.3 Interop with Iceberg and Delta (Specified)', style_h2))
    story.append(Paragraph(
        'Pond-native tables (DuckLake-style) have the cleanest one-copy '
        'story: writes go through the four syscalls, sealed objects are '
        'Parquet on S3, the catalog is content-addressed metadata on S3. '
        'Iceberg and Delta tables are read-mostly interop: Pond can read '
        'them via the <code>formats/iceberg.so</code> and <code>formats/'
        'delta.so</code> plugins. Writes to Iceberg are supported via '
        'the DuckDB <code>iceberg</code> extension (v1.4+, merge-on-read, '
        'positional deletes only). Writes to Delta are not yet supported '
        'upstream in DuckDB; this is a gap that may close in future '
        'DuckDB releases. For workloads that require Delta writes today, '
        'the recommended path is to write to a Pond-native table and '
        'mirror to Delta via a sink plugin.',
        style_body))
    return story


def section_7_versioning():
    story = []
    story.append(Paragraph('7. Versioning &amp; Compatibility Rules', style_h1))

    story.append(Paragraph(
        'Architectures die when versioning is vague. LLVM succeeded '
        'because IR stability exists &mdash; IR generated a decade ago '
        'still runs on modern LLVM. Git succeeded because object format '
        'stability exists &mdash; commits made in 2005 are still readable '
        'today. Linux succeeded because syscall stability exists &mdash; '
        'binaries compiled for Linux 2.0 still run on Linux 6.x. Without '
        'compatibility rules, every release becomes painful, users stop '
        'upgrading, and the project fragments. Pond specifies '
        'compatibility rules for every versioned artifact.',
        style_body))

    story.append(Paragraph('7.1 The versioning matrix (Specified)', style_h2))

    ver_data = [
        ['Artifact', 'Version source', 'Compatibility rule'],
        ['Storage ABI (4 syscalls)',
         'Semantic versioning',
         'Additive only. New syscalls allowed in minor versions; existing '
         'syscalls never change signature or semantics. Like Linux syscalls.'],
        ['Object format (sealed blobs)',
         'Schema-id in object metadata',
         'Forward-compatible. New readers handle old objects; old readers '
         'skip unknown fields. Like Parquet.'],
        ['Pond IR',
         'IR version number in plan',
         'Forward + backward compatible within major version. Old IR runs '
         'on new runtime; new runtime can read old IR. Major version bumps '
         'are explicit migration events. Like LLVM IR.'],
        ['Schema (table columns)',
         'Schema-id in commit metadata',
         'Additive. Add columns, widen types. Destructive changes (drop, '
         'rename) require a new schema-id; old commits keep old schema. '
         'Like Iceberg schema evolution.'],
        ['Capability declarations',
         'Capability spec version',
         'Plugins declare which spec version they target. Runtime refuses '
         'incompatible plugins at load time.'],
        ['Catalog / metadata',
         'Commit hash',
         'Always content-addressed. Old commits are forever readable. '
         'No backward or forward compatibility issue &mdash; the hash '
         'is the version.'],
        ['Wire protocols (pgwire, Quack)',
         'Protocol version',
         'Standard protocol versioning. Backward compatibility required '
         'for any protocol that ships as v1.'],
    ]
    story.append(styled_table(ver_data,
        col_widths=[40*mm, 35*mm, CONTENT_W - 75*mm]))
    story.append(Paragraph(
        'Table 7.1: Compatibility rules for every versioned artifact.',
        style_caption))

    story.append(Paragraph('7.2 The stability promise (Specified)', style_h2))
    story.append(callout(
        'Any object sealed today is readable by any future version of '
        'Pond. Any IR generated today is executable by any future '
        'runtime within the same major version. Any capability plugin '
        'built against spec version N will load on any runtime that '
        'supports spec version N or later. These are not aspirations; '
        'they are release criteria. A release that violates any of '
        'them is a regression and is blocked from shipping.',
        bg=ACCENT_SOFT, prefix='Stability promise'))

    story.append(Paragraph('7.3 Migration events (Specified)', style_h2))
    story.append(Paragraph(
        'When a major version bump is required (e.g., Pond IR v2, '
        'which changes operator semantics incompatibly), the migration '
        'is explicit: a tool reads old IR and emits new IR; old sealed '
        'objects remain readable (object format stability is separate '
        'from IR stability). The migration tool is shipped with the '
        'new release. Users are not forced to migrate; old IR continues '
        'to run on old runtimes. This is the LLVM model: IR upgrades '
        'are opt-in, gradual, and reversible.',
        style_body))
    return story


def section_8_failure_domains():
    story = []
    story.append(Paragraph('8. Failure Domains (Storage-Relevant Subset)', style_h1))

    story.append(Paragraph(
        'Production systems are defined by their failure domains. The '
        'failure domain matrix below specifies, for each storage-relevant '
        'failure mode, what continues to work and what blocks. The full '
        'failure domain matrix (including Coordinator, Planner, and '
        'capability failures) is in RFC 5. The semantics here are '
        'Specified; the recovery protocols are Implementation-defined.',
        style_body))

    fd_data = [
        ['Failure', 'Reads', 'Writes', 'Streaming', 'Recovery'],
        ['S3 unavailable\n(whole region)',
         'Continue from NVMe cache (bounded by cache size)',
         'Block (cannot seal)',
         'Continue if OPEN objects fit in NVMe',
         'S3 recovery; replay OPEN logs'],
        ['S3 degraded\n(elevated latency)',
         'Continue (slower)',
         'Continue (slower)',
         'Continue (slower)',
         'n/a'],
        ['S3 object not found\n(hash mismatch)',
         'Raise NOT_FOUND (deterministic)',
         'n/a',
         'n/a',
         'Re-fetch from replication backend'],
        ['Raft quorum lost\n(for one shard)',
         'Continue from S3 (stale)',
         'Block for that shard',
         'Block for that shard',
         'Quorum recovery or manual intervention'],
        ['Root store quorum lost',
         'Continue from cached roots (stale)',
         'Block (all writes)',
         'Block (new commits)',
         'Quorum recovery'],
        ['Network partition\n(minority side)',
         'Continue (stale) from S3',
         'Block on minority side',
         'Block on minority side',
         'Heal -&gt; Raft catch-up'],
        ['Whole node crash',
         'Other nodes continue',
         'Other nodes continue (Raft)',
         'Other nodes continue',
         'Rebuild from S3 + replay OPEN log'],
        ['Disk full\n(local NVMe)',
         'Continue from S3',
         'Block; backpressure to client',
         'Block',
         'Compaction, then resume'],
    ]
    story.append(styled_table(fd_data,
        col_widths=[35*mm, 35*mm, 30*mm, 30*mm, CONTENT_W - 130*mm]))
    story.append(Paragraph(
        'Table 8.1: Storage-relevant failure domains. Full matrix in RFC 5.',
        style_caption))

    story.append(Paragraph('8.1 The single-region linearizability trade', style_h2))
    story.append(Paragraph(
        'Pond is single-region linearizable. Cross-region is read-your-'
        'writes via async Raft followers + S3 cross-region replication. '
        'This is the FoundationDB / Spanner / TigerBeetle pattern: '
        'strong consistency in the primary region; eventual (bounded by '
        'replication lag) consistency across regions. Globally-'
        'linearizable OLTP without specialized hardware (TrueTime, '
        'atomic clocks) does not exist; Pond does not claim it. '
        'Workloads that require globally-linearizable OLTP should use '
        'CockroachDB or Spanner and CDC into Pond.',
        style_body))

    story.append(Paragraph('8.2 The S3 latency floor (fundamental)', style_h2))
    story.append(callout(
        'Truly cold PK lookups &mdash; keys not in any NVMe index &mdash; '
        'are 5&ndash;30&nbsp;ms. This is the S3 GET floor and cannot be '
        'tuned away. The architecture mitigates this via NVMe-persistent '
        'sparse indexes (100&ndash;500&nbsp;us for indexed keys), but '
        'a key not in any index will pay the S3 GET cost. Workloads '
        'that require sub-millisecond cold lookups for all keys should '
        'use TigerBeetle alongside Pond for hot keys.',
        bg=ACCENT_SOFT, prefix='Fundamental constraint'))
    return story


def section_9_chaos():
    story = []
    story.append(Paragraph('9. Chaos Tests as Architectural Acceptance Tests', style_h1))

    story.append(Paragraph(
        'Every failure mode in section 8 must be executable as an '
        'automated chaos test. Chaos tests are not nice-to-haves; they '
        'are release blockers. A release that cannot demonstrate, via '
        'automated test, that it survives each failure mode does not '
        'ship. This is the discipline that separates production systems '
        'from research prototypes.',
        style_body))

    story.append(Paragraph('9.1 Required chaos tests (Specified)', style_h2))

    chaos_data = [
        ['Test', 'What it does', 'What it verifies'],
        ['kill_coordinator',
         'Kill the Coordinator process mid-transaction.',
         'Writes continue (single-shard via Raft); new Coordinator '
         'elected; in-flight txn_records resolved.'],
        ['lose_s3',
         'Make S3 unavailable mid-write.',
         'NVMe cache serves reads; writes block (not lose data); '
         'OPEN objects survive on NVMe.'],
        ['kill_placement',
         'Unload the placement capability.',
         'Falls back to S3 default placement; reads continue; writes '
         'continue without NVMe promotion.'],
        ['kill_gpu_backend',
         'Make the GPU backend unavailable.',
         'Planner re-routes OLAP queries to DuckDB; queries succeed '
         '(possibly slower).'],
        ['kill_index_plugin',
         'Unload the sparse index plugin.',
         'Planner falls back to ART (DuckDB default); point lookups '
         'succeed (slower).'],
        ['partition_minority',
         'Network-partition the minority side of a 3-node cluster.',
         'Minority unavailable; majority continues; on heal, Raft '
         'catches up.'],
        ['crash_node',
         'Hard-crash a node (kill -9).',
         'Other nodes continue; rebuild from S3 + replay OPEN log; '
         'cold restart &lt; 30 s at 1 PB.'],
        ['root_store_quorum_loss',
         'Lose root store quorum.',
         'Writes block; reads continue from cached roots; on quorum '
         'recovery, writes resume.'],
        ['seal_lag_burst',
         'Sustained write burst exceeding drain throughput.',
         'Backpressure engages; writes block (not lose data); '
         'no unbounded memory growth.'],
    ]
    story.append(styled_table(chaos_data,
        col_widths=[40*mm, 55*mm, CONTENT_W - 95*mm]))
    story.append(Paragraph(
        'Table 9.1: Required chaos tests. Each is a release blocker.',
        style_caption))

    story.append(Paragraph('9.2 How chaos tests are run', style_h2))
    story.append(Paragraph(
        'Chaos tests run in CI on every PR that touches the storage '
        'kernel, the Coordinator, or any capability that declares a '
        'fallback. A PR that fails a chaos test is blocked. The tests '
        'are deterministic: they seed the cluster with a fixed dataset, '
        'inject the failure at a fixed point, and verify the post-'
        'failure state matches the expected state. Non-determinism in '
        'chaos tests is a bug, not a feature &mdash; flaky chaos tests '
        'are fixed before any other work proceeds.',
        style_body))
    return story


def section_10_invariants():
    story = []
    story.append(Paragraph('10. Architectural Invariants (Storage)', style_h1))

    story.append(Paragraph(
        'The storage layer maintains the following invariants. Each is '
        'Specified: any implementation that violates an invariant is '
        'non-conformant. The invariants are formally stated; the proofs '
        'are sketched. A complete formal proof is the subject of a '
        'separate document.',
        style_body))

    inv_data = [
        ['#', 'Invariant', 'Statement'],
        ['1', 'One-copy',
         'Only SEALED blobs on object storage are canonical. Every '
         'other artifact (OPEN object log, catalog cache, index cache, '
         'MVs) is rebuildable in bounded time from sealed blobs.'],
        ['2', 'Immutability',
         'Sealed objects never change. The hash of a sealed object is '
         'the hash of its bytes, forever. A sealed object cannot be '
         'modified, only superseded by a new sealed object.'],
        ['3', 'Bounded recovery',
         'After crash, the system recovers to the last-sealed state in '
         'O(un-sealed operations), never O(total historical data). '
         'Cold restart is bounded by the size of the OPEN object log, '
         'not by the size of the dataset.'],
        ['4', 'Snapshot consistency',
         'Any Read at a commit hash returns a consistent snapshot. '
         'Concurrent writes do not affect a read once the read has '
         'resolved the name to a hash.'],
        ['5', 'Reference locality',
         'Name resolution is deterministic for a given commit. The same '
         'name resolves to the same hash for all readers at the same '
         'commit; cross-region readers may see a stale commit, but the '
         'resolution is still deterministic at that commit.'],
        ['6', 'Cache correctness',
         'Cache invalidation is never stale for committed state. A '
         'reader never sees a cached value that has been superseded by '
         'a committed write. The cache may be stale (showing an older '
         'commit) but never inconsistent (showing a state that never '
         'existed).'],
        ['7', 'Deterministic apply',
         'Given the same DAG state and the same query, any conformant '
         'runtime produces the same result, regardless of which '
         'plugins are loaded. Non-determinism (now(), random(), uuid()) '
         'is resolved at the adapter layer before the storage kernel '
         'sees the operation.'],
        ['8', 'Content-addressed integrity',
         'The hash of a sealed object is computed from its bytes. Any '
         'bit-rot or corruption is detectable by re-hashing. Mismatched '
         'hashes raise INTEGRITY_ERROR and the object is re-fetched '
         'from the replication backend.'],
    ]
    story.append(styled_table(inv_data,
        col_widths=[10*mm, 35*mm, CONTENT_W - 45*mm]))
    story.append(Paragraph(
        'Table 10.1: Storage-layer invariants. Each is Specified.',
        style_caption))
    return story


def section_11_perf():
    story = []
    story.append(Paragraph('11. Performance Budgets (Storage)', style_h1))

    story.append(Paragraph(
        'Performance budgets are targets to validate by benchmark, not '
        'architectural guarantees. The architecture permits these '
        'targets; the implementation must demonstrate them. A release '
        'that misses a target by more than 2&times; is investigated; '
        'a release that misses by 10&times; is blocked. The targets '
        'are anchored to measured numbers from DuckDB, DuckLake, '
        'pg_ducklake, and dragonboat research.',
        style_body))

    perf_data = [
        ['Operation', 'Target', 'Mechanism', 'Anchored to'],
        ['Commit (single-shard, LAN)',
         '&lt; 1 ms',
         'Raft quorum fsync on NVMe',
         'dragonboat: 100K+ logs/sec'],
        ['Commit (cross-AZ)',
         '1&ndash;2 ms',
         'Cross-AZ Raft round-trip',
         'Standard cross-AZ RTT'],
        ['Cross-node visibility',
         '1&ndash;2 ms',
         'Async Raft followers',
         'dragonboat replication'],
        ['Streaming freshness',
         '1&ndash;2 ms',
         'Tail-read OPEN object log',
         'Local NVMe read'],
        ['PK lookup (memory hit)',
         '5&ndash;30 &micro;s',
         'In-memory ART index',
         'DuckDB ART (v0.4.1+)'],
        ['PK lookup (NVMe index hit)',
         '100&ndash;500 &micro;s',
         'NVMe-persistent sparse index',
         'ClickHouse sparse index'],
        ['PK lookup (truly cold)',
         '5&ndash;30 ms',
         'S3 GET one Parquet row group',
         'S3 Standard GET p50'],
        ['OLAP scan (fresh data)',
         '&asymp; zero union tax',
         'DuckDB unions SEALED + OPEN',
         'DuckDB vectorized scan'],
        ['Seal (async)',
         '1&ndash;10 s',
         'Arrow IPC -&gt; Parquet + S3 PUT',
         'pg_ducklake: 5.8&times; faster than vanilla DuckDB+DuckLake'],
        ['Cold restart',
         '&lt; 30 s at 1 PB',
         'Bounded log replay',
         'To validate in Phase-0 benchmark'],
        ['Failover',
         '&lt; 1 s',
         'Follower already has OPEN log',
         'Raft leader election'],
    ]
    story.append(styled_table(perf_data,
        col_widths=[45*mm, 28*mm, 45*mm, CONTENT_W - 118*mm]))
    story.append(Paragraph(
        'Table 11.1: Performance budgets. Targets, not guarantees.',
        style_caption))

    story.append(Paragraph('11.1 Scale targets (honest)', style_h2))

    scale_data = [
        ['Scale', 'Sealed data on S3', 'OPEN objects (bounded)', 'Hot catalog cache', 'Status'],
        ['1 PB', '~1 PB', '~10 MB &ndash; 1 GB', '~1&ndash;10 GB', 'v1 target'],
        ['10 PB', '~10 PB', '~100 MB &ndash; 10 GB', '~10&ndash;50 GB', 'v1 target (sharded)'],
        ['100 PB', 'shard by database', 'shard by database', 'shard by database', 'Research'],
    ]
    story.append(styled_table(scale_data,
        col_widths=[20*mm, 35*mm, 35*mm, 35*mm, CONTENT_W - 125*mm]))
    story.append(Paragraph(
        'Table 11.2: Scale targets. Architecture permits large-scale '
        'deployments; practical limits depend on implementation quality '
        'in metadata scaling, compaction, and distributed execution.',
        style_caption))

    story.append(Paragraph('11.2 Commit rate targets (honest)', style_h2))
    story.append(Paragraph(
        'v1 targets tens of thousands of commits/sec per database, '
        'scaling linearly with database count (each database\'s root '
        'pointers live in one Raft group). Millions of commits/sec is '
        'research &mdash; it requires hierarchical namespaces, '
        'append-only pointer logs, or commit groups, none of which are '
        'in v1.',
        style_body))
    return story


def section_12_dependency_budget():
    story = []
    story.append(Paragraph('12. Dependency Budget (Not LOC Budget)', style_h1))

    story.append(Paragraph(
        'An earlier draft of this RFC specified a lines-of-code budget '
        'for the storage kernel (&lt; 2000 lines), the Coordinator '
        '(&lt; 1000 lines), the Planner (&lt; 500 lines), and the IR '
        'core (&lt; 3000 lines). On review, LOC budgets are the wrong '
        'discipline: 500 ugly lines may be worse than 900 beautiful '
        'lines, and a clear refactor that grows LOC by 20% may be '
        'strictly better than what it replaced. The right discipline '
        'is a <i>dependency budget</i> &mdash; what each component may '
        'depend on, architecturally. Dependencies are enforceable by '
        'code review and tooling; LOC is not.',
        style_body))

    story.append(Paragraph('12.1 The dependency budget (Specified)', style_h2))

    dep_data = [
        ['Component', 'May depend on', 'May NOT depend on'],
        ['Storage kernel',
         'The 4 syscalls. The lifecycle state machine. The DAG pattern. '
         'The root pointer namespace.',
         'SQL. The Planner. The Coordinator. Any capability plugin. '
         'Any specific replication algorithm (Raft is a plugin). '
         'Any specific format (Parquet is a plugin).'],
        ['Coordinator',
         'Graph execution. Transaction boundaries. Capability discovery.',
         'SQL. The Planner. Any specific capability. Cost estimation. '
         'Scheduling (backends schedule their own work).'],
        ['Planner',
         'Pass execution. Dependency ordering. Invariant verification.',
         'Cost estimation (passes do). Backend selection (passes do). '
         'Routing decisions (passes do). Any specific capability.'],
        ['Pond IR core',
         'The 15 operators. The type system. The 5 optimization '
         'invariants. The lowering contract.',
         'Any backend\'s native IR. Any specific capability. SQL '
         'parsing (wire capability).'],
    ]
    story.append(styled_table(dep_data,
        col_widths=[30*mm, 65*mm, CONTENT_W - 95*mm]))
    story.append(Paragraph(
        'Table 12.1: Dependency budget for each core component.',
        style_caption))

    story.append(Paragraph('12.2 Enforcement', style_h2))
    story.append(Paragraph(
        'The dependency budget is enforced by code review and by a '
        'lint tool that checks imports. A PR that adds a forbidden '
        'dependency to a core component is blocked. If a component '
        'genuinely needs a new dependency, the PR must either (a) '
        'extract the dependent code to a plugin, or (b) justify why '
        'the dependency budget should be amended (rare; requires '
        'architecture review). This is the Linux-kernel-ABI discipline: '
        'the core is protected from growth by structural enforcement, '
        'not by goodwill.',
        style_body))

    story.append(Paragraph('12.3 LOC as a guideline, not a rule', style_h2))
    story.append(Paragraph(
        'LOC is tracked as a guideline. The storage kernel is expected '
        'to be roughly 2000 lines, the Coordinator roughly 1000, the '
        'Planner roughly 500, the IR core roughly 3000. If a component '
        'significantly exceeds its guideline, the team investigates '
        'whether complexity has migrated in &mdash; but the component '
        'is not blocked solely on LOC. The dependency budget is the '
        'hard rule; LOC is the soft signal.',
        style_body))
    return story


def section_13_research():
    story = []
    story.append(Paragraph('13. Open Questions &amp; Research', style_h1))

    story.append(Paragraph(
        'The following items are Research: acknowledged unsolved, named '
        'honestly rather than pretended solved. They are not in v1. They '
        'are the multi-year frontier where Pond will either mature into '
        'a serious production system or remain a sound architecture on '
        'paper. Each is paired with the reason it is hard and the '
        'direction a solution might take.',
        style_body))

    res_data = [
        ['Research item', 'Why it is hard', 'Direction'],
        ['Hierarchical namespaces for\nmillions of commits/sec',
         'The root pointer namespace becomes a hotspot at very high '
         'commit rates. Single Raft group tops out at ~50K commits/sec.',
         'Hierarchical namespaces (root pointers are themselves content-'
         'addressed trees, cached aggressively). Append-only pointer '
         'logs. Commit groups (multiple txns share one commit record).'],
        ['Storage-aware replication\nprotocol',
         'Raft replicates log entries; immutable object fragments could '
         'be replicated more efficiently (content-addressed, K-of-N, '
         'no leader). This is the genuine novel research contribution.',
         'Content-addressed fragments (hash = address, like Git blobs). '
         'K-of-N quorum writes (any node accepts). Gossip-based '
         'membership. Erasure coding for sealed objects. Geographic '
         'locality. Closer to IPFS + Cassandra + BitTorrent than to Raft.'],
        ['Distributed execution\n(Exchange across nodes)',
         'The Exchange operator exists in the IR but v1 runs graphs '
         'single-node. Multi-node execution requires shuffle, skew '
         'handling, locality, adaptive repartition, spilling, '
         'backpressure.',
         'v2+. IR supports it; runtime does not yet. Spark, Flink, '
         'Materialize all spent years here. Pond will too.'],
        ['Cross-backend cost-based\nplanning',
         'Choosing between DuckDB, GPU, Velox, remote node, etc. for '
         'a subgraph requires a sophisticated optimizer. Nobody has '
         'solved this well; Spark Catalyst, Calcite, DuckDB optimizer '
         'all do cost-based within one engine, not across engines.',
         'Learned cost models (plugins contribute cost functions). '
         'Adaptive estimation. Exhaustive search with pruning. '
         'Multi-year effort.'],
        ['Streaming semantics\n(exactly-once, watermarks, late data)',
         'Materialize spent years on differential dataflow. Flink spent '
         'years. Timely Dataflow spent years. Streaming is genuinely '
         'hard research.',
         'v1 supports batch + simple streaming (poll-based tail reads). '
         'Full differential-dataflow-class streaming is multi-year '
         'research. Honestly acknowledged.'],
        ['Learned indexes',
         'Indexes that learn access patterns and restructure themselves. '
         'Research, not production-proven at lakehouse scale.',
         'Future <code>index/learned.so</code> plugin. The Index trait '
         'is Specified; implementations are plugins.'],
        ['Operator fusion and\nadaptive optimization',
         'Runtime feedback that fuses operators and re-plans mid-query. '
         'Umbra territory. Requires morsel-driven execution.',
         'Future compute plugins. The architecture permits it; the '
         'implementation is years away.'],
    ]
    story.append(styled_table(res_data,
        col_widths=[40*mm, 55*mm, CONTENT_W - 95*mm]))
    story.append(Paragraph(
        'Table 13.1: Research items. Acknowledged unsolved; multi-year frontier.',
        style_caption))

    story.append(Paragraph('13.1 What is explicitly NOT claimed', style_h2))
    story.append(Paragraph(
        'Pond does not claim to beat DuckDB at OLAP &mdash; DuckDB IS '
        'the reference backend. Pond does not claim TigerBeetle-class '
        'mechanical sympathy &mdash; measurable budgets are the target, '
        'and the default DuckDB backend may not meet TigerBeetle\'s bar '
        '(future backends might). Pond does not claim to subsume Spark, '
        'Flink, Kafka, or Databricks &mdash; it is defined by what it '
        'IS (a capability-oriented data runtime), not by what it '
        'replaces. Pond does not claim the storage-aware replication '
        'protocol is solved &mdash; it is named as research. Pond does '
        'not claim PB-scale is proven &mdash; the architecture permits '
        'it; the implementation must demonstrate it.',
        style_body))
    return story


def section_14_references():
    story = []
    story.append(Paragraph('14. References &amp; Cross-References', style_h1))

    story.append(Paragraph('14.1 The other four RFCs', style_h2))
    story.append(Paragraph(
        'This is RFC 1 of 5. The complete architecture is specified '
        'across five documents:',
        style_body))

    rfc_data = [
        ['RFC', 'Title', 'Status'],
        ['RFC 1', 'Storage &amp; Versioned State (this document)',
         'v1.0, architecture-frozen'],
        ['RFC 2', 'Planner &amp; IR Specification',
         'Planned'],
        ['RFC 3', 'Transaction &amp; Consistency Model',
         'Planned'],
        ['RFC 4', 'Capability &amp; Placement Model',
         'Planned'],
        ['RFC 5', 'Operational Architecture',
         'Planned'],
    ]
    story.append(styled_table(rfc_data,
        col_widths=[18*mm, CONTENT_W - 60*mm, 42*mm]))

    story.append(Paragraph('14.2 Projects that informed this design', style_h2))
    story.append(Paragraph(
        'Pond\'s architecture is a synthesis of ideas from many systems. '
        'The synthesis is original; the individual ideas are not. Credit '
        'where credit is due:',
        style_body))

    infl_data = [
        ['Project', 'What Pond borrowed', 'What Pond rejected'],
        ['Git',
         'Content-addressed DAG. blob/tree/commit/tag. Immutable history.',
         'NoSQL. No transactions. No concurrency.'],
        ['LLVM',
         'IR + lowering contract. PassManager model. Backend neutrality. '
         'IR stability across versions.',
         'Compiler-specific abstractions.'],
        ['Linux',
         'Syscall ABI stability. Kernel/userspace separation. Driver '
         'model. Dependency budget discipline.',
         'OS-level process scheduling.'],
        ['SQLite',
         'Embedded, single-file philosophy. Public-domain stability promise.',
         'Single-writer. No scale-out.'],
        ['TigerBeetle',
         'Deterministic state machine replication. Static memory. '
         'Cache-line-aligned records. Viewstamped Replication.',
         'Fixed schema. No SQL. Single-shard only.'],
        ['FoundationDB',
         'Log-as-recovery-buffer pattern. Sequencer. Layered SQL on '
         'ordered KV. Non-blocking OCC.',
         'KV-only data model.'],
        ['DuckDB',
         'Vectorized execution. SQL optimizer. Extension API. ART index. '
         'In-process embeddability.',
         'Single-process-write assumption. No streaming SQL.'],
        ['DuckLake v1.0',
         'SQL catalog + Parquet on object storage. Data inlining. '
         'Multi-writer via catalog DB.',
         'n/a (foundational).'],
        ['pg_ducklake',
         'Postgres access method pattern. Background maintenance worker.',
         'Tying to Postgres (Pond uses DuckDB-native catalog by default).'],
        ['pg_duckpipe',
         'Internal delta-processor pattern. <code>CREATE PIPESOURCE</code> '
         'DX.',
         'n/a (foundational).'],
        ['ClickHouse',
         'Sparse primary indexes. MergeTree compaction. SIMD scans.',
         'MergeTree-as-format (we use Parquet). MergeTree-as-engine '
         '(too monolithic).'],
        ['PocketBase',
         'Single-binary philosophy. Embedded or server. Zero config.',
         'Single-node only.'],
        ['Apache Iceberg',
         'Open table format. REST catalog spec. Snapshot semantics.',
         'Manifest explosion at PB scale. Catalog-as-separate-service.'],
        ['Databricks LTAP',
         'The "one copy" problem statement. The agent-native motivation.',
         'Two durable copies (Postgres + Iceberg). Minutes-of-sync-lag '
         'reality.'],
        ['Apache Fluss',
         'OPEN/SEALED streaming intuition. Sub-second freshness target.',
         'ZooKeeper dependency. Tablet/ISR machinery. JVM.'],
    ]
    story.append(styled_table(infl_data,
        col_widths=[30*mm, 70*mm, CONTENT_W - 100*mm]))
    story.append(Paragraph(
        'Table 14.1: Influences. Synthesis is original; individual ideas are not.',
        style_caption))

    story.append(Paragraph('14.3 The unique contribution', style_h2))
    story.append(callout(
        'Pond is a capability-oriented data runtime: a minimal core '
        'coordinating many execution strategies over a single versioned, '
        'immutable, one-copy object substrate, with everything staying '
        'local unless coordination is provably necessary. '
        '<b>One copy. Infinite execution. Zero coordination unless '
        'necessary.</b>',
        bg=ACCENT_SOFT, prefix='Pond'))

    story.append(Paragraph(
        'No existing system has this exact combination: one canonical '
        'copy, capability composition, a tiny coordinating core, '
        'local-first operation, and an architectural commitment to '
        'removing concepts rather than adding them. The individual '
        'pieces exist in many systems; the synthesis does not. If Pond '
        'succeeds, it will be because the synthesis holds &mdash; not '
        'because any single piece is novel.',
        style_body))

    story.append(Paragraph('14.4 Document status', style_h2))
    story.append(Paragraph(
        'This document is the formal specification of Pond\'s storage '
        'and Versioned State layer. It is intended to be precise enough '
        'that two independent implementations of the Specified behaviors '
        'would produce identical observable results. Implementation-'
        'defined behaviors are flagged; research items are acknowledged. '
        'Changes to Specified behaviors require a new version of this '
        'RFC and a migration plan. Changes to Implementation-defined '
        'behaviors do not. Changes to Research items are the expected '
        'output of future work.',
        style_body))

    story.append(Spacer(1, 6*mm))
    story.append(HRule(color=ACCENT, thickness=1, space_before=4, space_after=4))
    story.append(Paragraph(
        '<i>End of RFC 1. RFC 2 (Planner &amp; IR Specification) follows.</i>',
        style_caption))
    return story


# ---------------------------------------------------------------------------
# Document assembly
# ---------------------------------------------------------------------------

def build_document(output_path):
    doc = BaseDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN_L, rightMargin=MARGIN_R,
        topMargin=MARGIN_T, bottomMargin=MARGIN_B,
        title='Pond RFC 1: Storage & Versioned State',
        author='Pond Architecture',
        subject='Formal specification of the Pond storage ABI and Versioned State',
        creator='Z.ai',
    )

    frame_cover = Frame(MARGIN_L, MARGIN_B, CONTENT_W, PAGE_H - MARGIN_T - MARGIN_B,
                        leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0,
                        id='cover_frame')
    frame_body = Frame(MARGIN_L, MARGIN_B, CONTENT_W, PAGE_H - MARGIN_T - MARGIN_B,
                       leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0,
                       id='body_frame')

    cover_template = PageTemplate(id='Cover', frames=[frame_cover], onPage=cover_page)
    body_template = PageTemplate(id='Body', frames=[frame_body], onPage=body_page)
    doc.addPageTemplates([cover_template, body_template])

    story = []
    # Cover
    story.extend(build_cover())
    # Switch to body template
    story.append(NextPageTemplate('Body'))

    # Sections
    story.extend(section_1_introduction())
    story.extend(section_2_buckets())
    story.extend(section_3_syscalls())
    story.extend(section_4_lifecycle())
    story.extend(section_5_versioned_state())
    story.extend(section_6_layout())
    story.extend(section_7_versioning())
    story.extend(section_8_failure_domains())
    story.extend(section_9_chaos())
    story.extend(section_10_invariants())
    story.extend(section_11_perf())
    story.extend(section_12_dependency_budget())
    story.extend(section_13_research())
    story.extend(section_14_references())

    doc.build(story)
    return output_path


if __name__ == '__main__':
    out = '/home/z/my-project/download/pond_rfc1_storage_and_versioned_state.pdf'
    build_document(out)
    size = os.path.getsize(out)
    print(f'Generated: {out}')
    print(f'Size: {size:,} bytes ({size/1024:.1f} KB)')
