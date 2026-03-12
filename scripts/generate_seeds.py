#!/usr/bin/env python3
"""
Generate seed CSV files for the regulatory compliance demo.
Produces 8 CSV files with realistic chemical industry data.

Usage: python generate_seeds.py
"""

import csv
import os
import random
from datetime import date, timedelta

random.seed(42)

SEEDS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seeds")
os.makedirs(SEEDS_DIR, exist_ok=True)


def write_csv(filename, fieldnames, rows):
    path = os.path.join(SEEDS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")
    return rows


def random_date(start, end):
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))


# ---------------------------------------------------------------------------
# 1. CUSTOMERS
# ---------------------------------------------------------------------------
def generate_customers():
    customers_raw = [
        ("CUST-001", "Alpha Assembly Solutions", "EMEA", "Electronics Manufacturing", "procurement@alpha-assembly.com", "2018-03-15"),
        ("CUST-002", "Enthone", "Americas", "Surface Treatment", "compliance@enthone.com", "2017-06-01"),
        ("CUST-003", "Coventya", "EMEA", "Chemical Manufacturing", "regulatory@coventya.com", "2019-01-20"),
        ("CUST-004", "Electrolube", "EMEA", "Electronics Manufacturing", "sales@electrolube.co.uk", "2016-09-10"),
        ("CUST-005", "Kester", "Americas", "Electronics Manufacturing", "info@kester.com", "2015-11-30"),
        ("CUST-006", "Samsung Electronics", "Asia", "Electronics Manufacturing", "reg.affairs@samsung.com", "2020-02-14"),
        ("CUST-007", "Technoprobe", "EMEA", "Electronics Manufacturing", "quality@technoprobe.com", "2021-07-22"),
        ("CUST-008", "Hisconic", "Asia", "Electronics Manufacturing", "compliance@hisconic.com", "2022-04-05"),
        ("CUST-009", "TE Connectivity", "Americas", "Electronics Manufacturing", "ehs@te.com", "2016-01-18"),
        ("CUST-010", "MacDermid", "Americas", "Surface Treatment", "regulatory@macdermid.com", "2014-08-25"),
        ("CUST-011", "Atotech", "EMEA", "Surface Treatment", "product.stewardship@atotech.com", "2017-03-12"),
        ("CUST-012", "Henkel", "EMEA", "Adhesives", "sustainability@henkel.com", "2013-05-07"),
        ("CUST-013", "BASF", "EMEA", "Chemical Manufacturing", "reach@basf.com", "2012-10-01"),
        ("CUST-014", "Dow Chemical", "Americas", "Chemical Manufacturing", "product.safety@dow.com", "2014-02-28"),
        ("CUST-015", "3M", "Americas", "Chemical Manufacturing", "ehs.compliance@3m.com", "2015-07-19"),
        ("CUST-016", "Volvo", "EMEA", "Automotive", "substances@volvo.com", "2018-11-03"),
        ("CUST-017", "Honeywell", "Americas", "Aerospace", "regulatory@honeywell.com", "2016-06-15"),
        ("CUST-018", "DuPont", "Americas", "Chemical Manufacturing", "stewardship@dupont.com", "2013-09-22"),
        ("CUST-019", "Solvay", "EMEA", "Chemical Manufacturing", "compliance@solvay.com", "2019-04-11"),
        ("CUST-020", "Arkema", "EMEA", "Chemical Manufacturing", "reach.arkema@arkema.com", "2020-08-30"),
    ]
    fields = ["customer_id", "customer_name", "region", "industry", "contact_email", "active_since"]
    rows = [dict(zip(fields, c)) for c in customers_raw]
    return write_csv("customers.csv", fields, rows)


# ---------------------------------------------------------------------------
# 2. PRODUCTS
# ---------------------------------------------------------------------------
def generate_products():
    products_raw = [
        # Alpha - Soldering Materials
        ("PROD-001", "SAC305-FLUX", "SAC305 Lead-Free Solder Paste", "Alpha", "Soldering Materials", ""),
        ("PROD-002", "SAC387-PASTE", "SAC387 No-Clean Solder Paste", "Alpha", "Soldering Materials", ""),
        ("PROD-003", "OM338-PT", "OM338-PT Solder Paste Type 4", "Alpha", "Soldering Materials", ""),
        ("PROD-004", "CVP520-FLUX", "CVP-520 VOC-Free Flux", "Alpha", "Flux", ""),
        ("PROD-005", "EF6000-WIRE", "Telecore HF-850 Cored Wire", "Alpha", "Soldering Materials", ""),
        ("PROD-006", "ALPHA-LF300", "LF-300 Lead-Free Solder Bar", "Alpha", "Soldering Materials", ""),
        ("PROD-007", "SAC-PREFORM1", "SAC305 Solder Preforms", "Alpha", "Soldering Materials", ""),
        ("PROD-008", "NR205-FLUX", "NR205 No-Residue Flux", "Alpha", "Flux", ""),
        ("PROD-009", "ALPHA-HMP", "High Melting Point Solder Paste", "Alpha", "Soldering Materials", ""),
        ("PROD-010", "ALPHA-TACKY", "Tacky Flux TF-10", "Alpha", "Flux", ""),
        # Enthone - Electroplating Solutions & Surface Finishes
        ("PROD-011", "ENT-CUVIA", "Cuprotec HS Acid Copper", "Enthone", "Electroplating Solutions", ""),
        ("PROD-012", "ENT-NIKLAD", "Niklad ELV Electroless Nickel", "Enthone", "Electroplating Solutions", "7440-02-0"),
        ("PROD-013", "ENT-AUREL", "Aurelect PC Gold Plating", "Enthone", "Surface Finishes", "7440-57-5"),
        ("PROD-014", "ENT-ENIG", "ENIG Surface Finish Solution", "Enthone", "Surface Finishes", ""),
        ("PROD-015", "ENT-IMMSN", "Immersion Tin Process", "Enthone", "Surface Finishes", "7440-31-5"),
        ("PROD-016", "ENT-PALLAD", "Pallatec Palladium Strike", "Enthone", "Electroplating Solutions", "7440-05-3"),
        ("PROD-017", "ENT-ZINCAT", "Zincat HT Zinc Plating", "Enthone", "Electroplating Solutions", "7440-66-6"),
        ("PROD-018", "ENT-CHRM3", "Trivalent Chromium Process", "Enthone", "Surface Finishes", ""),
        ("PROD-019", "ENT-SNTRI", "SnTri Matte Tin Plating", "Enthone", "Electroplating Solutions", "7440-31-5"),
        ("PROD-020", "ENT-AGBRT", "Silver Bright Plating Bath", "Enthone", "Electroplating Solutions", "7440-22-4"),
        # Coventya - Electroplating Solutions
        ("PROD-021", "COV-TRIZINC", "TriZinc 300 Alkaline Zinc", "Coventya", "Electroplating Solutions", ""),
        ("PROD-022", "COV-NIKEL", "NiKel HE High-Efficiency Nickel", "Coventya", "Electroplating Solutions", "7440-02-0"),
        ("PROD-023", "COV-CHROM", "Chromitop TCP Passivate", "Coventya", "Surface Finishes", ""),
        ("PROD-024", "COV-CUPREX", "Cuprex Acid Copper Brightener", "Coventya", "Electroplating Solutions", "7440-50-8"),
        ("PROD-025", "COV-ZNNI", "ZnNi Alloy Plating Bath", "Coventya", "Electroplating Solutions", ""),
        ("PROD-026", "COV-SEAL", "Corrosion Seal Post-Treatment", "Coventya", "Coatings", ""),
        ("PROD-027", "COV-DEGAS", "Degreaser Ultra Alkaline", "Coventya", "Cleaning Agents", ""),
        ("PROD-028", "COV-STRIP", "Strip-It Copper Stripper", "Coventya", "Cleaning Agents", ""),
        # Electrolube - Conformal Coatings, Adhesives, Cleaning
        ("PROD-029", "ELB-APL", "APL Acrylic Conformal Coating", "Electrolube", "Conformal Coatings", ""),
        ("PROD-030", "ELB-UR", "UR Polyurethane Conformal Coating", "Electrolube", "Conformal Coatings", ""),
        ("PROD-031", "ELB-SCC3", "SCC3 Silicone Conformal Coating", "Electrolube", "Conformal Coatings", ""),
        ("PROD-032", "ELB-EA091", "Epoxy Adhesive EA091", "Electrolube", "Adhesives", ""),
        ("PROD-033", "ELB-TCC", "Thermal Conductive Compound", "Electrolube", "Adhesives", ""),
        ("PROD-034", "ELB-ULC", "Ultraclean PCB Cleaner", "Electrolube", "Cleaning Agents", ""),
        ("PROD-035", "ELB-SWA", "Safewash Anti-Static Cleaner", "Electrolube", "Cleaning Agents", ""),
        ("PROD-036", "ELB-ERSC", "Contact Treatment Grease", "Electrolube", "Coatings", ""),
        # Kester - Soldering & Flux
        ("PROD-037", "KST-256", "Kester 256 No-Clean Flux", "Kester", "Flux", ""),
        ("PROD-038", "KST-245", "Kester 245 No-Clean Flux", "Kester", "Flux", ""),
        ("PROD-039", "KST-NXG1", "NXG1 Lead-Free Solder Paste", "Kester", "Soldering Materials", ""),
        ("PROD-040", "KST-275", "Kester 275 Spirit Flux", "Kester", "Flux", ""),
        ("PROD-041", "KST-WIRE44", "Solder Wire 44 Activated Rosin", "Kester", "Soldering Materials", ""),
        ("PROD-042", "KST-R562", "R562 Water-Soluble Paste", "Kester", "Soldering Materials", ""),
        ("PROD-043", "KST-ULTRA", "Ultrapure Solder Bar", "Kester", "Soldering Materials", ""),
        ("PROD-044", "KST-951", "951 Soldering Flux Pen", "Kester", "Flux", ""),
        # MacDermid - Surface Treatment & Coatings
        ("PROD-045", "MCD-ENDURA", "Endura 350 Surface Treatment", "MacDermid", "Surface Finishes", ""),
        ("PROD-046", "MCD-OSP", "Organic Solderability Preservative", "MacDermid", "Surface Finishes", ""),
        ("PROD-047", "MCD-ETCHCU", "Micro-Etch Copper Treatment", "MacDermid", "Cleaning Agents", ""),
        ("PROD-048", "MCD-NIPH", "Electroless Nickel-Phosphorus", "MacDermid", "Electroplating Solutions", "7440-02-0"),
        ("PROD-049", "MCD-SHADOW", "Shadow Direct Metallization", "MacDermid", "Electroplating Solutions", ""),
        ("PROD-050", "MCD-M1", "M-Plate Immersion Silver", "MacDermid", "Surface Finishes", "7440-22-4"),
        ("PROD-051", "MCD-BONDER", "MetaBond Adhesion Promoter", "MacDermid", "Adhesives", ""),
        ("PROD-052", "MCD-RESIST", "Ultra Resist Photoresist", "MacDermid", "Coatings", ""),
        # Additional mixed products
        ("PROD-053", "ALPHA-CLEAN1", "Residue Cleaner RC-100", "Alpha", "Cleaning Agents", ""),
        ("PROD-054", "ENT-BARREL", "Barrel Copper Plating Solution", "Enthone", "Electroplating Solutions", ""),
        ("PROD-055", "COV-PASSIV", "Passivation Solution Blue", "Coventya", "Surface Finishes", ""),
        ("PROD-056", "ELB-TPR", "Two-Part Resin Encapsulant", "Electrolube", "Adhesives", ""),
        ("PROD-057", "KST-RFWAVE", "RF Wave Solder Paste", "Kester", "Soldering Materials", ""),
        ("PROD-058", "MCD-BLACKOX", "Black Oxide Treatment", "MacDermid", "Surface Finishes", ""),
        ("PROD-059", "ALPHA-STNCL", "Stencil Adhesive SA-200", "Alpha", "Adhesives", ""),
        ("PROD-060", "ENT-RHODEX", "Rhodium Plating Solution", "Enthone", "Electroplating Solutions", "7440-16-6"),
    ]
    fields = ["product_id", "product_code", "product_description", "business_unit", "product_category", "cas_registry"]
    rows = [dict(zip(fields, p)) for p in products_raw]
    return write_csv("products.csv", fields, rows)


# ---------------------------------------------------------------------------
# 3. REGULATORY LISTS
# ---------------------------------------------------------------------------
def generate_regulatory_lists():
    lists_raw = [
        ("LIST-01", "SVHC", "EU REACH SVHC Candidate List", "EU", "ECHA", "2025-06-15", 240),
        ("LIST-02", "RoHS", "EU RoHS Directive 2011/65/EU", "EU", "European Commission", "2024-12-01", 10),
        ("LIST-03", "TSCA_PBT", "TSCA PBT Chemicals", "US", "EPA", "2025-03-20", 5),
        ("LIST-04", "CA_PROP65", "California Proposition 65", "US", "OEHHA", "2025-09-01", 980),
        ("LIST-05", "K_REACH", "Korea Registration Evaluation Authorization of Chemicals", "KR", "Ministry of Environment", "2025-01-10", 510),
        ("LIST-06", "GADSL", "Global Automotive Declarable Substance List", "GLOBAL", "GASG", "2025-04-30", 170),
        ("LIST-07", "CEPA", "Canadian Environmental Protection Act", "CA", "Environment Canada", "2024-11-15", 150),
        ("LIST-08", "PFAS", "PFAS Chemical Action Lists", "US", "EPA", "2025-10-22", 12000),
        ("LIST-09", "EU_ODS", "EU Ozone Depleting Substances Regulation", "EU", "European Commission", "2024-07-01", 95),
        ("LIST-10", "IARC", "IARC Carcinogen Classifications", "GLOBAL", "WHO", "2025-02-28", 320),
        ("LIST-11", "China_Toxic", "Catalog of Toxic Chemicals Severely Restricted in China", "CN", "MEE", "2025-05-18", 40),
        ("LIST-12", "AD_DSL", "Aerospace Defense Declarable Substance List", "GLOBAL", "IAEG", "2025-08-01", 200),
        ("LIST-13", "TSCA_12B", "TSCA Section 12b Export Notification", "US", "EPA", "2025-06-30", 750),
        ("LIST-14", "China_HazChem", "Catalog of Hazardous Chemicals", "CN", "MEM", "2024-09-15", 2800),
        ("LIST-15", "TW_Toxic", "Taiwan Toxic and Concerned Chemical Substances", "TW", "MoE", "2025-03-01", 340),
    ]
    fields = ["list_id", "list_code", "list_name", "region", "authority", "last_updated", "substance_count"]
    rows = [dict(zip(fields, r)) for r in lists_raw]
    return write_csv("regulatory_lists.csv", fields, rows)


# ---------------------------------------------------------------------------
# 4. RESTRICTED SUBSTANCES
# ---------------------------------------------------------------------------
def generate_restricted_substances():
    substances_master = [
        ("Lead", "7439-92-1", [
            ("LIST-01", 1000, "candidate", "CMR"),
            ("LIST-02", 1000, "restricted", "CMR"),
            ("LIST-04", 0, "declarable", "CMR"),
            ("LIST-06", 1000, "declarable", "CMR"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-12", 1000, "restricted", "CMR"),
            ("LIST-14", 100, "restricted", "Toxic"),
        ]),
        ("Cadmium", "7440-43-9", [
            ("LIST-01", 100, "candidate", "CMR"),
            ("LIST-02", 100, "restricted", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-06", 100, "declarable", "CMR"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-12", 100, "restricted", "CMR"),
        ]),
        ("Mercury", "7439-97-6", [
            ("LIST-01", 1000, "banned", "CMR"),
            ("LIST-02", 1000, "restricted", "Toxic"),
            ("LIST-04", 0, "declarable", "CMR"),
            ("LIST-07", 10, "restricted", "Toxic"),
            ("LIST-11", 10, "banned", "Toxic"),
        ]),
        ("Hexavalent Chromium", "18540-29-9", [
            ("LIST-01", 1000, "candidate", "CMR"),
            ("LIST-02", 1000, "restricted", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-06", 1000, "restricted", "CMR"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
        ]),
        ("Polybrominated Biphenyls (PBBs)", "59536-65-1", [
            ("LIST-02", 1000, "restricted", "PBT"),
            ("LIST-06", 1000, "restricted", "PBT"),
        ]),
        ("Polybrominated Diphenyl Ethers (PBDEs)", "32534-81-9", [
            ("LIST-02", 1000, "restricted", "PBT"),
            ("LIST-06", 1000, "restricted", "PBT"),
            ("LIST-03", 500, "restricted", "PBT"),
        ]),
        ("PFOA", "335-67-1", [
            ("LIST-01", 25, "banned", "vPvB"),
            ("LIST-08", 25, "restricted", "PBT"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-07", 25, "restricted", "PBT"),
        ]),
        ("PFOS", "1763-23-1", [
            ("LIST-01", 10, "banned", "vPvB"),
            ("LIST-08", 10, "restricted", "PBT"),
            ("LIST-11", 10, "banned", "PBT"),
        ]),
        ("Bisphenol A", "80-05-7", [
            ("LIST-01", 0, "candidate", "EDC"),
            ("LIST-04", 0, "declarable", "EDC"),
            ("LIST-05", 100, "restricted", "EDC"),
        ]),
        ("DEHP (Di-2-ethylhexyl phthalate)", "117-81-7", [
            ("LIST-01", 1000, "candidate", "CMR"),
            ("LIST-02", 1000, "restricted", "CMR"),
            ("LIST-04", 0, "declarable", "CMR"),
            ("LIST-06", 1000, "restricted", "CMR"),
        ]),
        ("DBP (Dibutyl phthalate)", "84-74-2", [
            ("LIST-01", 1000, "candidate", "CMR"),
            ("LIST-02", 1000, "restricted", "CMR"),
            ("LIST-04", 0, "declarable", "CMR"),
        ]),
        ("BBP (Benzyl butyl phthalate)", "85-68-7", [
            ("LIST-01", 1000, "candidate", "CMR"),
            ("LIST-02", 1000, "restricted", "CMR"),
            ("LIST-04", 0, "declarable", "CMR"),
        ]),
        ("DIBP (Diisobutyl phthalate)", "84-69-5", [
            ("LIST-01", 1000, "candidate", "CMR"),
            ("LIST-02", 1000, "restricted", "CMR"),
        ]),
        ("Toluene", "108-88-3", [
            ("LIST-04", 0, "declarable", "CMR"),
            ("LIST-13", 5000, "declarable", "Toxic"),
            ("LIST-14", 1000, "restricted", "Toxic"),
        ]),
        ("Benzene", "71-43-2", [
            ("LIST-01", 5, "candidate", "Carcinogen"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-14", 10, "restricted", "Carcinogen"),
        ]),
        ("Formaldehyde", "50-00-0", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-05", 100, "restricted", "CMR"),
        ]),
        ("Asbestos (Chrysotile)", "12001-29-5", [
            ("LIST-01", 0, "banned", "Carcinogen"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-11", 0, "banned", "Carcinogen"),
        ]),
        ("DecaBDE", "1163-19-5", [
            ("LIST-01", 500, "restricted", "PBT"),
            ("LIST-03", 500, "restricted", "PBT"),
            ("LIST-06", 500, "restricted", "PBT"),
        ]),
        ("PIP (3:1)", "13674-84-5", [
            ("LIST-03", 1000, "restricted", "PBT"),
            ("LIST-13", 1000, "declarable", "PBT"),
        ]),
        ("2,4,6-Tri-tert-butylphenol (2,4,6-TTBP)", "732-26-3", [
            ("LIST-03", 300, "restricted", "PBT"),
            ("LIST-01", 300, "candidate", "vPvB"),
        ]),
        ("Hexachlorobutadiene (HCBD)", "87-68-3", [
            ("LIST-01", 0, "banned", "PBT"),
            ("LIST-03", 100, "restricted", "PBT"),
            ("LIST-11", 50, "banned", "PBT"),
        ]),
        ("Pentachlorophenol", "87-86-5", [
            ("LIST-01", 5, "banned", "PBT"),
            ("LIST-03", 5, "restricted", "PBT"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
        ]),
        ("Diisocyanates (MDI)", "101-68-8", [
            ("LIST-01", 0, "candidate", "Sensitizer"),
            ("LIST-04", 0, "declarable", "Sensitizer"),
            ("LIST-06", 1000, "declarable", "Sensitizer"),
        ]),
        ("Nickel", "7440-02-0", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-05", 500, "restricted", "CMR"),
        ]),
        ("Cobalt dichloride", "7646-79-9", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
        ]),
        ("Arsenic", "7440-38-2", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-14", 100, "restricted", "Toxic"),
        ]),
        ("Antimony trioxide", "1309-64-4", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
        ]),
        ("Tributyltin compounds (TBT)", "688-73-3", [
            ("LIST-01", 0, "banned", "PBT"),
            ("LIST-11", 10, "banned", "Toxic"),
            ("LIST-15", 50, "restricted", "PBT"),
        ]),
        ("Nonylphenol", "25154-52-3", [
            ("LIST-01", 0, "candidate", "EDC"),
            ("LIST-05", 100, "restricted", "EDC"),
            ("LIST-07", 100, "restricted", "EDC"),
        ]),
        ("Octylphenol", "27193-28-8", [
            ("LIST-01", 0, "candidate", "EDC"),
            ("LIST-05", 100, "restricted", "EDC"),
        ]),
        ("Diarsenic trioxide", "1327-53-3", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
        ]),
        ("Trichloroethylene", "79-01-6", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
        ]),
        ("Methylene chloride (DCM)", "75-09-2", [
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-14", 500, "restricted", "Toxic"),
        ]),
        ("Carbon tetrachloride", "56-23-5", [
            ("LIST-09", 0, "banned", "Toxic"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-14", 10, "banned", "Toxic"),
        ]),
        ("1,1,1-Trichloroethane", "71-55-6", [
            ("LIST-09", 0, "banned", "Toxic"),
            ("LIST-07", 0, "banned", "Toxic"),
        ]),
        ("HCFC-141b", "1717-00-6", [
            ("LIST-09", 0, "restricted", "Toxic"),
            ("LIST-07", 0, "restricted", "Toxic"),
        ]),
        ("N-Methyl-2-pyrrolidone (NMP)", "872-50-4", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-06", 3000, "declarable", "CMR"),
        ]),
        ("Dimethylformamide (DMF)", "68-12-2", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "CMR"),
            ("LIST-14", 500, "restricted", "Toxic"),
        ]),
        ("Perfluorohexanoic acid (PFHxA)", "307-24-4", [
            ("LIST-08", 25, "restricted", "PBT"),
            ("LIST-01", 25, "candidate", "vPvB"),
        ]),
        ("GenX (HFPO-DA)", "13252-13-6", [
            ("LIST-08", 10, "restricted", "PBT"),
            ("LIST-04", 0, "declarable", "Toxic"),
        ]),
        ("Strontium chromate", "7789-06-2", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-12", 100, "restricted", "CMR"),
        ]),
        ("Chromium trioxide", "1333-82-0", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-12", 100, "restricted", "CMR"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
        ]),
        ("Boric acid", "10043-35-3", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "CMR"),
            ("LIST-05", 500, "restricted", "CMR"),
        ]),
        ("Sodium dichromate", "10588-01-9", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
        ]),
        ("Diboron trioxide", "1303-86-2", [
            ("LIST-01", 0, "candidate", "CMR"),
        ]),
        ("Triethyl phosphate", "78-40-0", [
            ("LIST-13", 5000, "declarable", "Toxic"),
            ("LIST-14", 1000, "restricted", "Toxic"),
        ]),
        ("Short-chain chlorinated paraffins (SCCPs)", "85535-84-8", [
            ("LIST-01", 0, "banned", "PBT"),
            ("LIST-06", 1500, "restricted", "PBT"),
            ("LIST-11", 50, "banned", "PBT"),
        ]),
        ("Medium-chain chlorinated paraffins (MCCPs)", "85681-73-8", [
            ("LIST-01", 0, "candidate", "vPvB"),
            ("LIST-06", 1500, "declarable", "PBT"),
        ]),
        ("4,4'-Diaminodiphenylmethane (MDA)", "101-77-9", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
        ]),
        ("Diethylhexyl phthalate (DEHA)", "103-23-1", [
            ("LIST-01", 0, "candidate", "EDC"),
        ]),
        ("Cyclohexane", "110-82-7", [
            ("LIST-14", 2000, "restricted", "Toxic"),
        ]),
        ("Ethylene glycol monoethyl ether", "110-80-5", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "CMR"),
        ]),
        ("Acrylamide", "79-06-1", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
        ]),
        ("Beryllium", "7440-41-7", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-12", 1000, "restricted", "CMR"),
        ]),
        ("Selenium", "7782-49-2", [
            ("LIST-04", 0, "declarable", "Toxic"),
            ("LIST-14", 500, "restricted", "Toxic"),
        ]),
        ("Thallium", "7440-28-0", [
            ("LIST-04", 0, "declarable", "Toxic"),
            ("LIST-14", 100, "restricted", "Toxic"),
        ]),
        ("Bis(2-methoxyethyl) ether (Diglyme)", "111-96-6", [
            ("LIST-01", 0, "candidate", "CMR"),
        ]),
        ("1,2-Dichloroethane", "107-06-2", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
        ]),
        ("Vinyl chloride", "75-01-4", [
            ("LIST-04", 0, "declarable", "Carcinogen"),
            ("LIST-10", 0, "declarable", "Carcinogen"),
            ("LIST-14", 10, "banned", "Carcinogen"),
        ]),
        ("Hydrazine", "302-01-2", [
            ("LIST-01", 0, "candidate", "CMR"),
            ("LIST-04", 0, "declarable", "Carcinogen"),
        ]),
        ("PFBS", "375-73-5", [
            ("LIST-01", 0, "candidate", "vPvB"),
            ("LIST-08", 50, "restricted", "PBT"),
        ]),
        ("6:2 Fluorotelomer alcohol (6:2 FTOH)", "647-42-7", [
            ("LIST-08", 100, "restricted", "PBT"),
        ]),
        ("Dimethyl sulfoxide (DMSO)", "67-68-5", [
            ("LIST-14", 5000, "declarable", "Toxic"),
        ]),
        ("Isopropanol", "67-63-0", [
            ("LIST-04", 0, "declarable", "Toxic"),
        ]),
        ("Rosin (Colophony)", "8050-09-7", [
            ("LIST-06", 5000, "declarable", "Sensitizer"),
        ]),
        ("Adipic acid", "124-04-9", [
            ("LIST-13", 10000, "declarable", "Toxic"),
        ]),
        ("Tin", "7440-31-5", [
            ("LIST-14", 5000, "declarable", "Toxic"),
            ("LIST-15", 2000, "restricted", "Toxic"),
        ]),
        ("Copper", "7440-50-8", [
            ("LIST-14", 5000, "declarable", "Toxic"),
        ]),
        ("Zinc", "7440-66-6", [
            ("LIST-14", 5000, "declarable", "Toxic"),
        ]),
        ("Silver", "7440-22-4", [
            ("LIST-14", 5000, "declarable", "Toxic"),
        ]),
    ]

    fields = ["substance_id", "substance_name", "cas_number", "list_id", "threshold_ppm", "restriction_type", "hazard_class"]
    rows = []
    sid = 1
    substance_id_map = {}
    substance_list_ids = {}

    for name, cas, entries in substances_master:
        for list_id, threshold, rtype, hazard in entries:
            sub_id = f"SUB-{sid:04d}"
            rows.append({
                "substance_id": sub_id,
                "substance_name": name,
                "cas_number": cas,
                "list_id": list_id,
                "threshold_ppm": threshold,
                "restriction_type": rtype,
                "hazard_class": hazard,
            })
            substance_id_map[(name, cas, list_id)] = sub_id
            key = (name, cas)
            if key not in substance_list_ids:
                substance_list_ids[key] = []
            substance_list_ids[key].append(sub_id)
            sid += 1

    write_csv("restricted_substances.csv", fields, rows)
    return rows, substance_id_map, substance_list_ids


# ---------------------------------------------------------------------------
# 5. PRODUCT SUBSTANCES
# ---------------------------------------------------------------------------
def generate_product_substances(products, substance_list_ids):
    heavy_metals = [
        ("Lead", "7439-92-1"),
        ("Cadmium", "7440-43-9"),
        ("Mercury", "7439-97-6"),
        ("Hexavalent Chromium", "18540-29-9"),
        ("Nickel", "7440-02-0"),
        ("Arsenic", "7440-38-2"),
        ("Beryllium", "7440-41-7"),
    ]
    solder_materials = [
        ("Lead", "7439-92-1"),
        ("Tin", "7440-31-5"),
        ("Silver", "7440-22-4"),
        ("Copper", "7440-50-8"),
        ("Rosin (Colophony)", "8050-09-7"),
    ]
    phthalates = [
        ("DEHP (Di-2-ethylhexyl phthalate)", "117-81-7"),
        ("DBP (Dibutyl phthalate)", "84-74-2"),
        ("BBP (Benzyl butyl phthalate)", "85-68-7"),
        ("DIBP (Diisobutyl phthalate)", "84-69-5"),
    ]
    pfas = [
        ("PFOA", "335-67-1"),
        ("PFOS", "1763-23-1"),
        ("PFBS", "375-73-5"),
        ("Perfluorohexanoic acid (PFHxA)", "307-24-4"),
    ]
    solvents = [
        ("Toluene", "108-88-3"),
        ("Benzene", "71-43-2"),
        ("Formaldehyde", "50-00-0"),
        ("N-Methyl-2-pyrrolidone (NMP)", "872-50-4"),
        ("Dimethylformamide (DMF)", "68-12-2"),
        ("Isopropanol", "67-63-0"),
        ("Methylene chloride (DCM)", "75-09-2"),
    ]
    flame_retardants = [
        ("Polybrominated Biphenyls (PBBs)", "59536-65-1"),
        ("Polybrominated Diphenyl Ethers (PBDEs)", "32534-81-9"),
        ("DecaBDE", "1163-19-5"),
        ("PIP (3:1)", "13674-84-5"),
        ("Antimony trioxide", "1309-64-4"),
    ]
    plating_chemicals = [
        ("Nickel", "7440-02-0"),
        ("Chromium trioxide", "1333-82-0"),
        ("Boric acid", "10043-35-3"),
        ("Cobalt dichloride", "7646-79-9"),
        ("Zinc", "7440-66-6"),
        ("Copper", "7440-50-8"),
    ]
    coating_chemicals = [
        ("Diisocyanates (MDI)", "101-68-8"),
        ("Bisphenol A", "80-05-7"),
        ("Formaldehyde", "50-00-0"),
        ("Nonylphenol", "25154-52-3"),
        ("N-Methyl-2-pyrrolidone (NMP)", "872-50-4"),
    ]

    category_substances = {
        "Soldering Materials": [solder_materials, heavy_metals[:3], flame_retardants[:2]],
        "Flux": [solder_materials[:1], solvents[:4], [("Rosin (Colophony)", "8050-09-7")]],
        "Electroplating Solutions": [plating_chemicals, heavy_metals[:4], solvents[:2]],
        "Surface Finishes": [plating_chemicals[:3], heavy_metals[:4], pfas[:1]],
        "Cleaning Agents": [solvents, pfas[:2], [("Nonylphenol", "25154-52-3")]],
        "Adhesives": [coating_chemicals, phthalates[:2], solvents[:3]],
        "Coatings": [coating_chemicals, phthalates, flame_retardants[:3]],
        "Conformal Coatings": [coating_chemicals, solvents[:4], pfas[:1]],
    }

    data_sources = ["Sphera Cloud", "ChemGes", "Chemmate", "SDS Review", "Lab Analysis", "Palantir Foundry"]
    fields = ["product_substance_id", "product_id", "substance_id", "concentration_ppm", "last_verified_date", "data_source"]
    rows = []
    ps_id = 1

    for prod in products:
        pid = prod["product_id"]
        cat = prod["product_category"]
        groups = category_substances.get(cat, [solvents, heavy_metals[:2]])

        all_candidates = []
        seen = set()
        for g in groups:
            for s in g:
                if s not in seen and s in substance_list_ids:
                    seen.add(s)
                    all_candidates.append(s)

        n_substances = random.randint(3, min(8, len(all_candidates)))
        chosen = random.sample(all_candidates, min(n_substances, len(all_candidates)))

        for sub_key in chosen:
            sub_ids = substance_list_ids[sub_key]
            sub_id = random.choice(sub_ids)

            roll = random.random()
            if roll < 0.15:
                concentration = random.randint(800, 5000)
            elif roll < 0.30:
                concentration = random.randint(200, 999)
            else:
                concentration = random.randint(0, 150)

            verified_date = random_date(date(2024, 1, 1), date(2025, 12, 31))

            rows.append({
                "product_substance_id": f"PS-{ps_id:04d}",
                "product_id": pid,
                "substance_id": sub_id,
                "concentration_ppm": concentration,
                "last_verified_date": verified_date.isoformat(),
                "data_source": random.choice(data_sources),
            })
            ps_id += 1

    write_csv("product_substances.csv", fields, rows)
    return rows


# ---------------------------------------------------------------------------
# 6. SURVEY REQUESTS
# ---------------------------------------------------------------------------
def generate_survey_requests(customers):
    survey_types = ["SVHC", "RoHS", "TSCA_PBT", "CA_PROP65", "Full_Compliance", "RSL_Declaration", "Excel_Survey"]
    complexities = ["easy", "mid-complex", "complex"]
    statuses = ["completed", "in_progress", "pending", "cancelled"]
    business_units = ["Alpha", "Enthone", "Coventya", "Electrolube", "Kester", "MacDermid"]

    fields = ["survey_id", "customer_id", "region", "survey_type", "complexity", "product_count",
              "status", "requested_date", "due_date", "completed_date", "business_unit"]
    rows = []

    for i in range(1, 81):
        cust = random.choice(customers)
        complexity = random.choice(complexities)
        if complexity == "easy":
            product_count = random.randint(1, 5)
        elif complexity == "mid-complex":
            product_count = random.randint(4, 12)
        else:
            product_count = random.randint(8, 20)

        status = random.choices(statuses, weights=[40, 25, 25, 10])[0]
        req_date = random_date(date(2025, 1, 1), date(2026, 2, 15))
        due_date = req_date + timedelta(days=random.randint(14, 90))

        completed_date = ""
        if status == "completed":
            completed_date = random_date(req_date, min(due_date, date(2026, 2, 15))).isoformat()

        rows.append({
            "survey_id": f"SRV-{i:04d}",
            "customer_id": cust["customer_id"],
            "region": cust["region"],
            "survey_type": random.choice(survey_types),
            "complexity": complexity,
            "product_count": product_count,
            "status": status,
            "requested_date": req_date.isoformat(),
            "due_date": due_date.isoformat(),
            "completed_date": completed_date,
            "business_unit": random.choice(business_units),
        })

    write_csv("survey_requests.csv", fields, rows)
    return rows


# ---------------------------------------------------------------------------
# 7. SURVEY RESPONSES
# ---------------------------------------------------------------------------
def generate_survey_responses(surveys, products, restricted_substances, substance_list_ids):
    analysts = ["J. Chen", "M. Kim", "S. Patel", "L. Mueller", "A. Santos", "R. Tanaka", "P. Dubois"]

    active_surveys = [s for s in surveys if s["status"] in ("completed", "in_progress")]

    all_sub_ids = []
    for ids_list in substance_list_ids.values():
        all_sub_ids.extend(ids_list)

    fields = ["response_id", "survey_id", "product_id", "substance_id", "compliance_status",
              "declared_concentration_ppm", "response_date", "analyst"]
    rows = []
    rid = 1

    for survey in active_surveys:
        n_responses = random.randint(2, 6)
        survey_date = date.fromisoformat(survey["requested_date"])
        due = date.fromisoformat(survey["due_date"])

        for _ in range(n_responses):
            prod = random.choice(products)
            sub_id = random.choice(all_sub_ids)

            roll = random.random()
            if roll < 0.15:
                c_status = "non_compliant"
                conc = random.randint(500, 5000)
            elif roll < 0.30:
                c_status = "flagged"
                conc = random.randint(200, 1200)
            elif roll < 0.38:
                c_status = "under_review"
                conc = random.randint(50, 800)
            else:
                c_status = "compliant"
                conc = random.randint(0, 200)

            resp_date = random_date(survey_date, min(due, date(2026, 2, 15)))

            rows.append({
                "response_id": f"RSP-{rid:04d}",
                "survey_id": survey["survey_id"],
                "product_id": prod["product_id"],
                "substance_id": sub_id,
                "compliance_status": c_status,
                "declared_concentration_ppm": conc,
                "response_date": resp_date.isoformat(),
                "analyst": random.choice(analysts),
            })
            rid += 1

    write_csv("survey_responses.csv", fields, rows)
    return rows


# ---------------------------------------------------------------------------
# 8. REGULATORY DATA SOURCES
# ---------------------------------------------------------------------------
def generate_regulatory_data_sources():
    sources_raw = [
        ("SRC-01", "ECHA", "government", "EU", "https://echa.europa.eu", "daily", "2026-02-15"),
        ("SRC-02", "EUR-Lex", "government", "EU", "https://eur-lex.europa.eu", "weekly", "2026-02-10"),
        ("SRC-03", "EPA", "government", "US", "https://www.epa.gov", "daily", "2026-02-16"),
        ("SRC-04", "OEHHA", "government", "US", "https://oehha.ca.gov", "monthly", "2026-01-31"),
        ("SRC-05", "ChemLinked", "commercial", "GLOBAL", "https://www.chemlinked.com", "daily", "2026-02-15"),
        ("SRC-06", "Chemical Watch", "news", "GLOBAL", "https://chemicalwatch.com", "daily", "2026-02-16"),
        ("SRC-07", "Enhesa", "consultant", "GLOBAL", "https://www.enhesa.com", "weekly", "2026-02-12"),
        ("SRC-08", "REACHLaw", "consultant", "EU", "https://www.reachlaw.fi", "monthly", "2026-01-20"),
        ("SRC-09", "CIRS ChemRadar", "commercial", "GLOBAL", "https://www.cirs-group.com", "weekly", "2026-02-09"),
        ("SRC-10", "NITE-CHRIP", "government", "JP", "https://www.nite.go.jp/en/chem/chrip", "monthly", "2026-01-15"),
        ("SRC-11", "REACH24H", "consultant", "GLOBAL", "https://www.reach24h.com", "weekly", "2026-02-08"),
        ("SRC-12", "Enviliance", "commercial", "GLOBAL", "https://enviliance.com", "daily", "2026-02-16"),
        ("SRC-13", "GPC Gateway", "commercial", "GLOBAL", "https://www.gpcgateway.com", "weekly", "2026-02-07"),
        ("SRC-14", "Sphera Cloud", "internal", "GLOBAL", "https://sphera.cloud.internal", "daily", "2026-02-16"),
        ("SRC-15", "Chemmate", "internal", "GLOBAL", "https://chemmate.internal", "daily", "2026-02-16"),
        ("SRC-16", "Palantir Foundry", "internal", "GLOBAL", "https://foundry.internal", "real-time", "2026-02-16"),
        ("SRC-17", "SciFinder", "commercial", "GLOBAL", "https://scifinder.cas.org", "daily", "2026-02-15"),
        ("SRC-18", "Korea MEE", "government", "KR", "https://www.me.go.kr", "monthly", "2026-01-28"),
        ("SRC-19", "Environment Canada", "government", "CA", "https://www.canada.ca/en/environment-climate-change", "monthly", "2026-01-30"),
        ("SRC-20", "Taiwan MoE", "government", "TW", "https://www.moenv.gov.tw", "quarterly", "2025-12-15"),
        ("SRC-21", "China MEE", "government", "CN", "https://www.mee.gov.cn", "monthly", "2026-01-25"),
        ("SRC-22", "China MEM", "government", "CN", "https://www.mem.gov.cn", "monthly", "2026-01-20"),
        ("SRC-23", "ChemGes", "commercial", "GLOBAL", "https://www.dr-software.com/chemges", "weekly", "2026-02-11"),
        ("SRC-24", "IAEG", "consultant", "GLOBAL", "https://www.iaeg.com", "quarterly", "2025-11-30"),
        ("SRC-25", "GASG", "consultant", "GLOBAL", "https://www.gadsl.org", "semi-annual", "2025-10-15"),
        ("SRC-26", "WHO IARC", "government", "GLOBAL", "https://monographs.iarc.who.int", "annual", "2025-06-01"),
        ("SRC-27", "Toxnet (NLM)", "government", "US", "https://www.nlm.nih.gov/toxnet", "monthly", "2026-01-18"),
        ("SRC-28", "OECD eChemPortal", "government", "GLOBAL", "https://www.echemportal.org", "weekly", "2026-02-06"),
        ("SRC-29", "PubChem", "government", "US", "https://pubchem.ncbi.nlm.nih.gov", "daily", "2026-02-15"),
        ("SRC-30", "CompTox Dashboard", "government", "US", "https://comptox.epa.gov/dashboard", "monthly", "2026-01-22"),
    ]
    fields = ["source_id", "source_name", "source_type", "region", "url", "update_frequency", "last_checked"]
    rows = [dict(zip(fields, s)) for s in sources_raw]
    return write_csv("regulatory_data_sources.csv", fields, rows)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("Generating seed CSV files...")
    print(f"Output directory: {SEEDS_DIR}\n")

    customers = generate_customers()
    products = generate_products()
    generate_regulatory_lists()
    restricted_substances, substance_id_map, substance_list_ids = generate_restricted_substances()
    generate_product_substances(products, substance_list_ids)
    surveys = generate_survey_requests(customers)
    generate_survey_responses(surveys, products, restricted_substances, substance_list_ids)
    generate_regulatory_data_sources()

    print("\nAll seed files generated successfully.")


if __name__ == "__main__":
    main()
