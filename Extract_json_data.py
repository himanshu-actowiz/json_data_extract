import json
import datetime
import calendar

base_path = "C:/Users/hemanshu.marwadi/Desktop/Extract_json_data/data-2026216105652.json"
data_dict = dict()
#Convert Json File to Python File
def read_json_data(base_path):
    with open(base_path , "r") as file:
        json_data = json.load(file)
        return json_data
    
#Extract json data to give a Schema format
def extract_json_data(raw_json_data):
    data_dict['restaurant_id'] = raw_json_data['page_info']['resId']
    data_dict['restaurant_name'] = raw_json_data['page_data']['sections']['SECTION_BASIC_INFO']['name']
    data_dict['restaurant_url'] = raw_json_data['page_info']['canonicalUrl']
    data_dict['restaurant_contact'] = raw_json_data['page_data']['sections']['SECTION_RES_CONTACT']['phoneDetails']['phoneStr']
    data_dict['fssai_licence_number'] = raw_json_data['page_data']['order']['menuList']['fssaiInfo']['text']
    information = raw_json_data['page_data']['sections']['SECTION_RES_CONTACT']
    
    data_dict['address_info'] = {
        'full_address':information['address'],
        'region':information['country_name'],
        'city':information['city_name'],
        'pincode' : information['zipcode'],
        'state' : 'Gujarat'
    }
    
    cuisines = []
    for i in raw_json_data['page_data']['sections']['SECTION_RES_HEADER_DETAILS']['CUISINES']:
            cuisines.append({
                'name' : i['name'],
                'url' : i['url']
            })
            
    data_dict['cuisines'] = cuisines
    time_structure = dict()
    timing_data = raw_json_data['page_data']['sections']['SECTION_BASIC_INFO']['timing']['customised_timings']['opening_hours'][0]['timing']
    open_time = timing_data.split(" ")[0]
    close_time = timing_data.split(" ")[2]
    
    #Find out of Weekday name using number(calendar)
    for i in range(0,7):
        day_name = calendar.day_name[i]
        time_structure[day_name] ={
            'open' : open_time,
            'close' : close_time
        }
    
    menu_category = []
    menus = raw_json_data['page_data']['order']['menuList']['menus']
    
    #Find Sub Categories
    for menu in menus:
        categories = menu['menu']['categories']
        
        for category in categories:
            category_data = {
                "category_name": category["category"]["name"],
                "items": []
            }
            items = category['category']['items']
            for item_obj in items:
                item = item_obj["item"]
                #Get Sub item Information
                items_data = {
                    "item_id": item['id'],
                    "item_name": item["name"],
                    "item_slugs": item["tag_slugs"],
                    "item_description": item["desc"],
                    "is_veg": bool(item["dietary_slugs"])
                }
                category_data["items"].append(items_data)
            menu_category.append(category_data)
        data_dict['menu_categories'] = menu_category
    return data_dict
    print(data_dict)
current_date= datetime.date.today() #Find Current Date

#Convert Python File into Json File 

def convert_json_file(convert_json_data):
    with open(f"ZOMATO_{current_date}.json" , "w") as file:
        json.dump(convert_json_data,file,indent=4)
            
raw_json_data=read_json_data(base_path)
struct=extract_json_data(raw_json_data)
convert_json_file(struct)
