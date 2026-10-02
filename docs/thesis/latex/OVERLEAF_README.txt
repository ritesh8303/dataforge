DataForge UE Master's thesis — Overleaf pack
Date: 2 October 2026
Author: Ritesh Rakesh Jadhav

HOW TO UPLOAD
1. Overleaf → New Project → Upload Project → this zip.
2. Menu → Settings:
   - Main document: overleaf_main.tex
   - Compiler: pdfLaTeX
   - TeX Live: latest
3. Click Recompile. Overleaf will run pdflatex + bibtex automatically.

FILL BEFORE BINDING (top of overleaf_main.tex)
- \Matrikel{...}
- \Erstbetreuung{...}
- \Zweitbetreuung{...}
- \Abgabedatum{...}

FILES IN THIS ZIP (compile set only)
- overleaf_main.tex
- preamble.tex
- references.bib
- chapters/*.tex used by the main file
- figures/architecture.png
- figures/rq3_bars.png

Do not set main.tex, ieee_short_paper.tex, or colloquium_slides.tex as the Main document.
Those files are not in this zip on purpose, so the project compiles as the thesis.

Rewrite the body in your own voice before submission (UE AI-writing rule).
