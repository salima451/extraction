import streamlit as st
import pandas as pd
from io import BytesIO

# --------------------------
# Fonctions de parsing générique
# --------------------------
def parse_full_hl7(hl7_message):
    """
    Parse complet du message HL7 en séparant chaque ligne par le délimiteur "|".
    Retourne un DataFrame où chaque ligne du message correspond à une ligne du tableau.
    """
    rows = []
    lines = hl7_message.strip().splitlines()
    for line in lines:
        fields = line.split("|")
        rows.append(fields)
    max_fields = max(len(row) for row in rows)
    for row in rows:
        if len(row) < max_fields:
            row.extend([""] * (max_fields - len(row)))
    col_names = [f"Field {i+1}" for i in range(max_fields)]
    return pd.DataFrame(rows, columns=col_names)

# --------------------------
# Parsing pour ORLine
# --------------------------
def parse_details_hl7_orline(hl7_message):
    """
    Extrait les détails spécifiques du message HL7 pour ORLine et retourne un dictionnaire.
    """
    data = {}
    lignes = hl7_message.strip().splitlines()
    
    for ligne in lignes:
        champs = ligne.strip().split("|")
        segment = champs[0]
        
        if segment == "PID":
            if len(champs) > 2:
                data["ID PAT"] = champs[2]
                
        elif segment == "PV1":
            if len(champs) > 2:
                data["Admission Entree"] = champs[2]
            if len(champs) > 18:
                data["ID Sejour"] = champs[18]
                
        elif segment == "SCH":
            if len(champs) > 1:
                data["ID Operation"] = champs[1].split('^')[0]
            if len(champs) > 11:
                sous_champs = champs[11].split('^')
                if len(sous_champs) > 3:
                    dt = sous_champs[3]
                    if len(dt) >= 8:
                        date_str = dt[:8]
                        formatted_date = date_str[6:8] + "/" + date_str[4:6] + "/" + date_str[0:4]
                        data["Dat Operation"] = formatted_date
                        
        elif segment == "OBX":
            if len(champs) > 1 and champs[1] == "2":
                if len(champs) > 5:
                    data["Cod Service Entree"] = champs[5]
                    
        elif segment == "AIL":
            if len(champs) > 3:
                champ_ail = champs[3]
                if "." in champ_ail:
                    splitted_dot = champ_ail.split(".", 1)
                    ail_after_dot = splitted_dot[1] if len(splitted_dot) > 1 else ""
                    splitted_caret = ail_after_dot.split("^^^", 1)
                    data["Cod Service Entree"] = splitted_caret[0].strip()
                    if len(splitted_caret) > 1:
                        base_service = splitted_caret[1].split("^")[0].strip()
                        data["Service Entree"] = "^^^" + base_service
                    else:
                        data["Service Entree"] = ""
                else:
                    data["Cod Service Entree"] = champ_ail
                    data["Service Entree"] = ""
                    
        elif segment == "PV2":
            if "Type d'hospitalisation" not in data:
                data["Type d'hospitalisation"] = "(Donnée correcte extraite de PV1-2)"
                
    return data

# --------------------------
# Parsing pour WISH
# --------------------------
def parse_details_hl7_wish(hl7_message):
    """
    Extrait les détails spécifiques du message HL7 pour WISH et retourne un dictionnaire.
    
    - ID PAT : PID Field 4 (index 3)
    - Date de naissance : PID Field 8 (index 7), format jj/mm/aaaa
    - Sexe : PID Field 9 (index 8)
    - Date Message : MSH Field 7 (index 6), format jj/mm/aaaa
    - Heure Message : MSH Field 7 (index 6), format hh:mm
    """
    data = {}
    lignes = hl7_message.strip().splitlines()
    
    for ligne in lignes:
        champs = ligne.strip().split("|")
        segment = champs[0]
        
        if segment == "MSH":
            if len(champs) > 6:
                dt_str = champs[6]
                if len(dt_str) >= 8:
                    annee = dt_str[0:4]
                    mois  = dt_str[4:6]
                    jour  = dt_str[6:8]
                    data["Date Message"] = f"{jour}/{mois}/{annee}"
                    if len(dt_str) >= 12:
                        heure = dt_str[8:10]
                        minute = dt_str[10:12]
                        data["Heure Message"] = f"{heure}:{minute}"
                    else:
                        data["Heure Message"] = ""
        
        elif segment == "PID":
            if len(champs) > 3:
                data["ID PAT"] = champs[3]
            if len(champs) > 7:
                dob_str = champs[7]
                if len(dob_str) >= 8:
                    annee = dob_str[0:4]
                    mois  = dob_str[4:6]
                    jour  = dob_str[6:8]
                    data["Date Naissance"] = f"{jour}/{mois}/{annee}"
            if len(champs) > 8:
                data["Sexe"] = champs[8]
                
    return data

# --------------------------
# Sélection dynamique du parseur
# --------------------------
def parse_details_hl7_dynamic(hl7_message, source):
    if source == "ORLine":
        return parse_details_hl7_orline(hl7_message)
    elif source == "WISH":
        return parse_details_hl7_wish(hl7_message)
    else:
        return {}

def to_excel(df):
    """
    Convertit un DataFrame en fichier Excel et retourne les bytes.
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Parsed HL7")
    return output.getvalue()

# --------------------------
# Interface principale
# --------------------------
def main():
    st.title("📂 HL7 Data Analyzer - WISH & ORLine")
    
    # Utiliser la source comme clé pour réinitialiser l'upload lors d'un changement
    source_choice = st.radio("Sélectionnez la source HL7 :", ("WISH", "ORLine"))
    
    st.header("🏥 HL7 Data Parser & Patient Analysis")
    
    # Ajout d'un key dynamique pour le file uploader basé sur la source sélectionnée
    uploaded_files = st.file_uploader(
        "📌 Sélectionnez un ou plusieurs fichiers HL7", 
        type=["txt", "hl7", "dat"], 
        accept_multiple_files=True,
        key=f"file_uploader_{source_choice}"
    )
    
    if uploaded_files:
        details_list = []
        full_parsed_list = []
        
        for uploaded_file in uploaded_files:
            file_bytes = uploaded_file.getvalue()
            try:
                hl7_message = file_bytes.decode("utf-8")
            except UnicodeDecodeError:
                hl7_message = file_bytes.decode("latin-1")
            
            # Parsing complet
            df_full = parse_full_hl7(hl7_message)
            df_full["Fichier"] = uploaded_file.name
            full_parsed_list.append(df_full)
            
            # Extraction des détails selon la source choisie
            details = parse_details_hl7_dynamic(hl7_message, source_choice)
            details["Fichier"] = uploaded_file.name
            details["Source HL7"] = source_choice
            details_list.append(details)
        
        if full_parsed_list:
            df_full_combined = pd.concat(full_parsed_list, ignore_index=True, sort=False)
            st.subheader("📄 Messages HL7 parsés")
            st.dataframe(df_full_combined)
            
            excel_data = to_excel(df_full_combined)
            st.download_button(
                label="💾 Télécharger les messages HL7 complets parsés en Excel",
                data=excel_data,
                file_name="parsed_hl7_complet.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        details_df = pd.DataFrame(details_list)
        if "ID PAT" in details_df.columns:
            patient_ids = details_df["ID PAT"].dropna().unique().tolist()
            selected_patient = st.selectbox("📌 Sélectionnez un ID PATIENT pour afficher ses détails", options=patient_ids)
            if selected_patient:
                patient_details = details_df[details_df["ID PAT"] == selected_patient]
                st.subheader(f"📋 Détails pour le patient {selected_patient}")
                st.dataframe(patient_details)
        else:
            st.warning("❌ Aucun ID PAT trouvé dans les fichiers HL7 uploadés.")

if __name__ == "__main__":
    main()
