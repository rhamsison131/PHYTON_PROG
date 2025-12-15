import requests
import base64
import json
from ord_schema.proto import reaction_pb2
from rdkit import Chem
from google.protobuf.json_format import MessageToDict

AMINE_SMARTS = Chem.MolFromSmarts("[NX3;H2,H1;!$(NC=O)]")
ARYL_HALIDE_SMARTS = Chem.MolFromSmarts("[c]~[F,Cl,Br,I]")
CARBOXYLIC_ACID_SMARTS = Chem.MolFromSmarts("[CX3](=O)[OX2H1]")

def get_smiles(component):
    for identifier in component.identifiers:
        if identifier.type == reaction_pb2.CompoundIdentifier.SMILES:
            return identifier.value
    return None

def is_metal(mol):
    if not mol: return False
    for atom in mol.GetAtoms():
        if atom.GetAtomicNum() in [3, 11, 19, 37, 55, 12, 20, 38, 56, 26, 27, 28, 29, 30, 44, 45, 46, 47, 48, 76, 77, 78, 79, 80]:
            return True
    return False

def classify_component(component, role, input_key=""):
    smiles = get_smiles(component)
    mol = Chem.MolFromSmiles(smiles) if smiles else None
    key_lower = input_key.lower()
    
    categories = []
    
    if role == reaction_pb2.ReactionRole.SOLVENT:
        categories.append("Solvent")
        return categories
    
    if "carboxylic acid" in key_lower or "acid" in key_lower:
        if mol and mol.HasSubstructMatch(CARBOXYLIC_ACID_SMARTS):
            categories.append("Carboxylic Acid")
        elif "carboxylic acid" in key_lower:
            categories.append("Carboxylic Acid")
            
    if "amine" in key_lower:
        categories.append("Amine")
        
    if "activation" in key_lower or "coupling agent" in key_lower:
        categories.append("Activation Agent")
        
    if "additive" in key_lower:
        categories.append("Additive")
        
    if "base" in key_lower:
        categories.append("Base")
        
    if "ligand" in key_lower:
        categories.append("Ligand")
        
    if "catalyst" in key_lower or "metal" in key_lower:
        if is_metal(mol):
            categories.append("Metal")
        else:
            categories.append("Ligand")

    if role == reaction_pb2.ReactionRole.CATALYST and "Metal" not in categories and "Ligand" not in categories:
        if is_metal(mol):
            categories.append("Metal")
        else:
            categories.append("Ligand")
            
    if role == reaction_pb2.ReactionRole.REAGENT:
        if "Base" not in categories and "Activation Agent" not in categories and "Additive" not in categories:
            categories.append("Base")
        
    if role == reaction_pb2.ReactionRole.REACTANT:
        if mol:
            if "Amine" not in categories and mol.HasSubstructMatch(AMINE_SMARTS):
                categories.append("Amine")
            if "Aryl Halide" not in categories and mol.HasSubstructMatch(ARYL_HALIDE_SMARTS):
                categories.append("Aryl Halide")
            if "Carboxylic Acid" not in categories and mol.HasSubstructMatch(CARBOXYLIC_ACID_SMARTS):
                categories.append("Carboxylic Acid")
    
    if key_lower.startswith("m"):
        parts = key_lower.split('_')
        for part in parts:
            if part.startswith("m") and part[1:].isdigit():
                m_key = part.upper()
                categories.append(m_key)
            
    return list(set(categories))

import sys

def main():
    datasets_to_process = []
    
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            target_id = None
            if "ord_dataset-" in arg:
                start_idx = arg.find("ord_dataset-")
                possible_id = arg[start_idx:]
                for char in ['/', '?', '&', ' ']:
                    if char in possible_id:
                        possible_id = possible_id.split(char)[0]
                target_id = possible_id.strip()
            
            if target_id:
                if not any(d['dataset_id'] == target_id for d in datasets_to_process):
                    datasets_to_process.append({'dataset_id': target_id})
            else:
                print(f"Warning: Could not extract dataset ID from '{arg}'")
    
    if datasets_to_process:
        print(f"Targeting {len(datasets_to_process)} specific datasets.")
    else:
        print("No specific datasets provided, fetching all available for default behavior...")
        datasets_url = "https://open-reaction-database.org/api/datasets"
        try:
            datasets = requests.get(datasets_url).json()
            datasets_to_process = datasets[:2]
        except Exception as e:
            print(f"Error fetching datasets: {e}")
            return

    organized_data = {
        "Base": [],
        "Solvent": [],
        "Amine": [],
        "Aryl Halide": [],
        "Metal": [],
        "Ligand": [],
        "Carboxylic Acid": [],
        "Additive": [],
        "Activation Agent": [],
        "M1": [], "M2": [], "M3": [], "M4": [],
        "M5": [], "M6": [], "M7": [], "M8": [], "M9": []
    }

    for dataset_info in datasets_to_process:
        dataset_id = dataset_info['dataset_id']
        print(f"Processing dataset {dataset_id}...")
        
        query_url = "https://open-reaction-database.org/api/query"
        params = {
            "dataset_id": dataset_id,
            "limit": 50
        }
        
        try:
            response = requests.get(query_url, params=params)
            results = response.json()
        except Exception as e:
            print(f"Error querying dataset {dataset_id}: {e}")
            continue
        
        for result in results:
            reaction_id = result['reaction_id']
            proto_str = result['proto']
            
            try:
                proto_bytes = base64.b64decode(proto_str)
                reaction = reaction_pb2.Reaction.FromString(proto_bytes)
                
                for input_key, input_val in reaction.inputs.items():
                    for component in input_val.components:
                        
                        raw_component_data = MessageToDict(
                            component, 
                            preserving_proto_field_name=True,
                            use_integers_for_enums=False
                        )
                        role_name = raw_component_data.get('reaction_role', 'UNSPECIFIED')
                        
                        categories = classify_component(component, component.reaction_role, input_key)
                        
                        identifier_type = "UNKNOWN"
                        value = "Unknown"
                        
                        for ident in component.identifiers:
                            if ident.type == reaction_pb2.CompoundIdentifier.SMILES:
                                identifier_type = "SMILES"
                                value = ident.value
                                break
                        
                        if identifier_type == "UNKNOWN":
                            for ident in component.identifiers:
                                if ident.type == reaction_pb2.CompoundIdentifier.NAME:
                                    identifier_type = "NAME"
                                    value = ident.value
                                    break
                        
                        for cat in categories:
                            if cat not in organized_data:
                                organized_data[cat] = []
                                
                            entry = {
                                "reaction_id": reaction_id,
                                "input_key": input_key,
                                "reaction_role": role_name,
                                "identifier_type": identifier_type,
                                "value": value,
                            }
                            organized_data[cat].append(entry)

            except Exception as e:
                print(f"Error processing reaction {reaction_id}: {e}")

    output_file = "ord_data.json"
    final_output = {"raw": organized_data}
    
    total_items = sum(len(v) for v in organized_data.values())
    print(f"Saving {total_items} classified components to {output_file}...")
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2)
    print("Done.")

if __name__ == "__main__":
    main()
