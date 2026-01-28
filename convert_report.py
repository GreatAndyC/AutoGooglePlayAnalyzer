import os
import glob
from markdown_pdf import MarkdownPdf, Section

def convert_latest_report():
    # Find the latest report
    list_of_files = glob.glob('reports/*.md') 
    if not list_of_files:
        print("No markdown reports found in reports/ directory.")
        return

    latest_file = max(list_of_files, key=os.path.getctime)
    print(f"Found latest report: {latest_file}")
    
    # Output filename
    output_pdf = latest_file.replace('.md', '.pdf')
    
    # Read markdown content
    with open(latest_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Create PDF
    pdf = MarkdownPdf(toc_level=2)
    pdf.add_section(Section(content, toc=False))
    
    # Save
    pdf.save(output_pdf)
    print(f"Successfully generated PDF: {output_pdf}")

if __name__ == "__main__":
    convert_latest_report()
