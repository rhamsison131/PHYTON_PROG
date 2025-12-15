import time
import json
import requests
import io
import xml.etree.ElementTree as ET
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException

def parse_xml_data(xml_content, source_url):
    try:
        root = ET.fromstring(xml_content)
        
        data = {
            "sequence_origin": source_url,
            "details_url": source_url,
            "smiles": "",
            "reaction_conditions": {},
            "yield": "",
            "source": {},
        }
        
        all_roles = {}
        
        reaction_smiles_elem = root.find(".//reactionSmiles")
        if reaction_smiles_elem is not None and reaction_smiles_elem.text:
            data["smiles"] = reaction_smiles_elem.text.strip()
        
        source_elem = root.find(".//source")
        if source_elem is not None:
            data["source"] = {
                "literature": source_elem.findtext("literatureSource", "").strip(),
                "DOI": source_elem.findtext("DOI", "").strip(),
                "year": source_elem.findtext("year", "").strip(),
                "date_added": source_elem.findtext("dateAdded", "").strip()
            }
        
        conditions_elem = root.find(".//reactionConditions")
        if conditions_elem is not None:
            data["reaction_conditions"] = {
                "time": conditions_elem.findtext("reactionTime/amount", "").strip(),
                "time_unit": conditions_elem.findtext("reactionTime/unit", "").strip(),
                "temperature": conditions_elem.findtext("reactionTemperature/amount", "").strip(),
                "temperature_unit": conditions_elem.findtext("reactionTemperature/unit", "").strip(),
                "temperature_start": conditions_elem.findtext("reactionTemperatureStart/amount", "").strip(),
                "temperature_end": conditions_elem.findtext("reactionTemperatureEnd/amount", "").strip(),
                "reflux": conditions_elem.findtext("reflux/amount", "").strip()
            }
        
        yield_elem = root.find(".//yield")
        if yield_elem is not None:
            data["yield"] = {
                "amount": yield_elem.findtext("amount", "").strip(),
                "unit": yield_elem.findtext("unit", "").strip()
            }
        
        participants_elem = root.find(".//participants")
        if participants_elem is not None:
            molecules = participants_elem.findall(".//molecule")
            print(f"  Found {len(molecules)} molecules")
            
            for molecule in molecules:
                role_elem = molecule.find("role")
                if role_elem is None or not role_elem.text:
                    continue
                    
                role = role_elem.text.strip().lower()
                role_key = role.replace(" ", "_")
                
                if role not in all_roles:
                    all_roles[role] = {
                        "list": [],
                        "primary_name": ""
                    }
                    data[f"{role_key}_details"] = []
                    data[role] = ""
                
                name_elem = molecule.find("name")
                smiles_elem = molecule.find("smiles")
                inchi_key_elem = molecule.find("inchiKey")
                ratio_elem = molecule.find("ratio")
                
                mol_info = {
                    "name": name_elem.text.strip() if name_elem is not None and name_elem.text else "Unknown",
                    "smiles": smiles_elem.text.strip() if smiles_elem is not None and smiles_elem.text else None,
                    "inchiKey": inchi_key_elem.text.strip() if inchi_key_elem is not None and inchi_key_elem.text else None,
                    "ratio": ratio_elem.text.strip() if ratio_elem is not None and ratio_elem.text else None,
                    "role": role,
                    "notes": None
                }
                
                for child in molecule:
                    if child.tag not in ["role", "name", "smiles", "inchiKey", "ratio"]:
                        if child.text and child.text.strip():
                            mol_info[child.tag] = child.text.strip()
                        elif child.attrib:
                            mol_info[f"{child.tag}_attrs"] = child.attrib
                
                data[f"{role_key}_details"].append(mol_info)
                all_roles[role]["list"].append(mol_info)
                
                if mol_info["name"] != "Unknown" and not all_roles[role]["primary_name"]:
                    all_roles[role]["primary_name"] = mol_info["name"]
                    data[role] = mol_info["name"]
                
                print(f"    {role}: {mol_info['name']}")
        
        if not data["smiles"]:
            reactant_roles = ["reactant", "building block", "starting material"]
            agent_roles = ["solvent", "reagent", "oxidizing agent", "catalyst", "base", "quench", "additive"]
            product_roles = ["product"]
            
            reactant_smiles = []
            agent_smiles = []
            product_smiles = []
            
            for role, role_data in all_roles.items():
                for mol in role_data["list"]:
                    if mol["smiles"]:
                        if role in reactant_roles:
                            reactant_smiles.append(mol["smiles"])
                        elif role in product_roles:
                            product_smiles.append(mol["smiles"])
                        else:
                            agent_smiles.append(mol["smiles"])
            
            if reactant_smiles or agent_smiles or product_smiles:
                data["smiles"] = f"{'.'.join(reactant_smiles)}>{'.'.join(agent_smiles)}>{'.'.join(product_smiles)}"
        
        print(f"    Summary of roles found: {', '.join(all_roles.keys())}")
        for role, role_data in all_roles.items():
            print(f"      {role}: {len(role_data['list'])} items")
        
        return data
        
    except ET.ParseError as e:
        print(f"!! XML Parse Error: {e}")
        try:
            xml_content = xml_content.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
            xml_content = xml_content.strip()
            
            if '<reaction' in xml_content and '</reaction>' in xml_content:
                start = xml_content.find('<reaction')
                end = xml_content.find('</reaction>') + len('</reaction>')
                xml_content = xml_content[start:end]
                
                return parse_xml_data(xml_content, source_url)
        except:
            pass
        
        return create_minimal_data(source_url)
    
    except Exception as e:
        print(f"!! Failed to parse XML: {e}")
        import traceback
        traceback.print_exc()
        return create_minimal_data(source_url)

def create_minimal_data(source_url):
    return {
        "sequence_origin": source_url,
        "details_url": source_url,
        "smiles": "",
        "reaction_conditions": {},
        "yield": "",
        "source": {}
    }

def get_user_selection():
    print("How would you like to select reaction data?")
    print(" - Single: Enter a number (e.g., 2 for 2nd reaction)")
    print(" - Range: Enter 'start-end' (e.g., 1-3 for reactions 1 to 3)")
    print(" - All: Enter 'all' to process all reactions")
    print(" - Multiple: Enter numbers separated by commas (e.g., 1,3,5)")
    
    while True:
        user_input = input("\nEnter your selection: ").strip().lower()
        
        if user_input in ["all"] or user_input.isdigit() or "-" in user_input or "," in user_input:
            return user_input
        else:
            print("Invalid input. Please try again.")
            continue

def get_reaction_indices(user_input, total_count):
    try:
        indices = []
        
        if user_input == "all":
            indices = list(range(total_count))
            print(f"✓ Selected all {total_count} reactions")
        
        elif "-" in user_input:
            parts = user_input.split("-")
            if len(parts) != 2:
                print("Invalid range format. Use 'start-end' (e.g., 1-3)")
                return None
            
            try:
                start = int(parts[0].strip()) - 1
                end = int(parts[1].strip())
                
                if start < 0 or end > total_count or start >= end:
                    print(f"Invalid range. Please enter numbers between 1 and {total_count}")
                    return None
                
                indices = list(range(start, end))
                print(f"✓ Selected reactions {start+1} to {end}")
            
            except ValueError:
                print("Invalid input. Range must be numbers (e.g., 1-3)")
                return None
        
        elif "," in user_input:
            try:
                numbers = [int(x.strip()) for x in user_input.split(",")]
                indices = [n - 1 for n in numbers if 1 <= n <= total_count]
                
                if not indices:
                    print(f"No valid numbers. Please enter numbers between 1 and {total_count}")
                    return None
                
                print(f"✓ Selected reactions: {', '.join([str(i+1) for i in indices])}")
            
            except ValueError:
                print("Invalid input. Enter numbers separated by commas (e.g., 1,3,5)")
                return None
        
        else:
            try:
                num = int(user_input)
                if 1 <= num <= total_count:
                    indices = [num - 1]
                    print(f"✓ Selected reaction #{num}")
                else:
                    print(f"Invalid number. Please enter a number between 1 and {total_count}")
                    return None
            
            except ValueError:
                print("Invalid input. Please enter a valid selection.")
                return None
        
        return indices
        
    except Exception as e:
        print(f"Error processing input: {e}")
        return None


def run_scraper():
    print(f"\n{'='*60}")
    print(f"                            CRD SCRAPER ")
    print(f"Developed by: BARRAL, Jacinth Cedric & LAROCO, Jan Lorenz")
    print(f"{'='*60}")
    
    user_selection = get_user_selection()
    
    print("\nOpening website to fetch reaction data list...")
    print("(Browser window will open now...)\n")
    
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    
    all_reaction_data = []

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    main_archive_url = "https://kmt.vander-lingen.nl/archive"

    try:
        driver.get(main_archive_url)
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "reaction data")))

        initial_links = driver.find_elements(By.PARTIAL_LINK_TEXT, "reaction data")
        total_count = len(initial_links)
        
        print(f"\nFound {total_count} reaction data sets available.")
        
        selected_indices = get_reaction_indices(user_selection, total_count)
        
        if selected_indices is None:
            print("Invalid selection. Exiting...")
            driver.quit()
            return
        
        print("\nStarting scrape process...")

        for i in selected_indices:
            try:
                current_links = driver.find_elements(By.PARTIAL_LINK_TEXT, "reaction data")
                if i >= len(current_links):
                    break
                
                target_link = current_links[i]
                print(f"\n=== SET {i+1}/{total_count}: {target_link.text} ===")
                
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target_link)
                target_link.click()
                
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                main_window_handle = driver.current_window_handle

                page_num = 1
                while True:
                    details_btns = driver.find_elements(By.LINK_TEXT, "Details")
                    if not details_btns:
                        print(f"  No Details buttons found on page {page_num}")
                        break
                    
                    print(f"  Page {page_num}: Found {len(details_btns)} reactions. Scanning...")

                    for j, btn in enumerate(details_btns):
                        try:
                            print(f"    Reaction {j+1}/{len(details_btns)}")
                            
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                            btn.click()
                            
                            WebDriverWait(driver, 5).until(EC.number_of_windows_to_be(2))
                            driver.switch_to.window(driver.window_handles[-1])
                            
                            time.sleep(1.5)
                            
                            try:
                                xml_element = WebDriverWait(driver, 5).until(
                                    EC.presence_of_element_located((By.LINK_TEXT, "XML"))
                                )
                                xml_url = xml_element.get_attribute("href")
                                current_url = driver.current_url
                                
                                print(f"      Fetching XML from: {xml_url}")

                                cookies = driver.get_cookies()
                                session = requests.Session()
                                for cookie in cookies:
                                    session.cookies.set(cookie['name'], cookie['value'])
                                
                                headers = {
                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                                }
                                response = session.get(xml_url, headers=headers, timeout=10)
                                
                                if response.status_code == 200:
                                    parsed_json = parse_xml_data(response.text, current_url)
                                    if parsed_json:
                                        all_reaction_data.append(parsed_json)
                                        
                                        roles_present = [k for k in parsed_json.keys() 
                                                         if k.endswith('_details') or 
                                                         (k not in ['sequence_origin', 'details_url', 'smiles', 
                                                                    'reaction_conditions', 'yield', 'source'] and 
                                                          not k.endswith('_details'))]
                                        summary = []
                                        for role in roles_present:
                                            if role.endswith('_details'):
                                                role_name = role.replace('_details', '')
                                                if role_name in parsed_json and parsed_json[role_name]:
                                                    summary.append(f"{role_name}: {parsed_json[role_name]}")
                                            elif parsed_json.get(role):
                                                summary.append(f"{role}: {parsed_json[role]}")
                                        
                                        if summary:
                                            print(f"      ✓ Extracted: {' | '.join(summary)}")
                                        else:
                                            print(f"      ✓ Extracted (no named compounds)")
                                            
                                        if len(all_reaction_data) % 5 == 0:
                                            with open("reaction_data_intermediate.json", "w", encoding="utf-8") as f:
                                                json.dump(all_reaction_data, f, indent=4)
                                            print(f"      (Auto-saved {len(all_reaction_data)} reactions)")
                                else:
                                    print(f"      X Failed to fetch XML: Status {response.status_code}")

                            except Exception as e:
                                print(f"      X Error processing XML: {e}")

                        finally:
                            if len(driver.window_handles) > 1:
                                driver.close()
                            driver.switch_to.window(main_window_handle)
                            time.sleep(0.5)
                    
                    try:
                        next_btn = driver.find_element(By.LINK_TEXT, "Next")
                        print(f"  Moving to next page...")
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                        next_btn.click()
                        time.sleep(2)
                        page_num += 1
                    except NoSuchElementException:
                        print("  > End of pages for this set.")
                        break

                print(f"  Returning to archive...")
                driver.get(main_archive_url)
                wait.until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "reaction data")))
                time.sleep(1)

            except Exception as e:
                print(f"Error in main loop: {e}")
                try:
                    driver.get(main_archive_url)
                    wait.until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "reaction data")))
                except:
                    pass
                continue

    finally:
        print(f"\nScraping Complete. Saving {len(all_reaction_data)} reactions to 'reaction_data.json'...")
        with open("reaction_data.json", "w", encoding="utf-8") as f:
            json.dump(all_reaction_data, f, indent=4, ensure_ascii=False)
        
        print(f"\n=== ANALYSIS OF ROLES FOUND ===")
        all_unique_roles = set()
        for reaction in all_reaction_data:
            for key in reaction:
                if key.endswith('_details'):
                    all_unique_roles.add(key.replace('_details', ''))
        
        print(f"Total unique roles found: {len(all_unique_roles)}")
        print(f"Roles: {', '.join(sorted(all_unique_roles))}")
        
        driver.quit()

if __name__ == "__main__":
    run_scraper()
