import json
import datetime
import calendar
import mysql.connector

base_path = r"C:\Users\hemanshu.marwadi\Desktop\ACTOWIZ-Himanshu\Extract_json_data\Zomato.json"
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
    data_dict['fssai_licence_number'] = raw_json_data['page_data']['order']['menuList']['fssaiInfo']['text'].split(' ')[2]
    information = raw_json_data['page_data']['sections']['SECTION_RES_CONTACT']
    
    data_dict['address_info'] = {
        'full_address':information['address'],
        'region':information['country_name'].replace('India','Ambli'),
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
    open_time = timing_data.split(" ")[0].replace('noon',"pm")
    close_time = timing_data.split(" ")[2]
    
    #Find out of Weekday name using number(calendar)
    for i in range(0,7):
        day_name = calendar.day_name[i]
        time_structure[day_name] ={
            'open' : open_time,
            'close' : close_time
        }
    data_dict['timings'] = time_structure
    menu_category = []
    menus = raw_json_data['page_data']['order']['menuList']['menus']
    
    previous_desc = None
    previous_category_name = None
    
    
    
    #Find Sub Categories
    for menu in menus:
        categories = menu['menu']['categories']
        main_category_name = menu['menu']['name']
        # print(main_category_name)
        
        for category in categories:
            current_c_name = category["category"]["name"]
            if not current_c_name:
                current_c_name = main_category_name
            category_data = {
                "category_name": current_c_name,
                "items": []
            }
            items = category['category']['items']
            for item_obj in items:
                item = item_obj["item"]
                current_desc = item.get("desc")
                if not current_desc:
                    current_desc = previous_desc
                else:
                    previous_desc = current_desc 
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
current_date= datetime.date.today() #Find Current Date

#Convert Python File into Json File 

def convert_json_file(convert_json_data):
    with open(f"ZOMATO_{current_date}.json" , "w") as file:
        json.dump(convert_json_data,file,indent=4)
            
raw_json_data=read_json_data(base_path)
struct=extract_json_data(raw_json_data)
convert_json_file(struct)


conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="actowiz",
    database="Extract_Json_Databse"
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS Restaurant_information(
    restaurant_id BIGINT PRIMARY KEY,
    restaurant_name VARCHAR(255),
    restaurant_url TEXT,
    restaurant_contact VARCHAR(20),
    fssai_licence_number VARCHAR(50),
    full_address TEXT,
    region VARCHAR(100),
    city VARCHAR(100),
    pincode VARCHAR(20),
    state VARCHAR(100),
    cuisines JSON,
    timings JSON
);
""")


if isinstance(struct, dict):
    struct = [struct]

for restaurant in struct:

    cursor.execute("""
    INSERT IGNORE INTO Restaurant_information (
        restaurant_id,
        restaurant_name,
        restaurant_url,
        restaurant_contact,
        fssai_licence_number,
        full_address,
        region,
        city,
        pincode,
        state,
        cuisines,
        timings
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        restaurant.get("restaurant_id"),
        restaurant.get("restaurant_name"),
        restaurant.get("restaurant_url"),
        restaurant.get("restaurant_contact"),
        restaurant.get("fssai_licence_number"),
        restaurant.get("address_info", {}).get("full_address"),
        restaurant.get("address_info", {}).get("region"),
        restaurant.get("address_info", {}).get("city"),
        restaurant.get("address_info", {}).get("pincode"),
        restaurant.get("address_info", {}).get("state"),
        json.dumps(restaurant.get("cuisines")),
        json.dumps(restaurant.get("timings"))
    ))

conn.commit()
print("Restaurant Data Inserted Successfully")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Restaurant_Menu(
    item_id VARCHAR(50) ,
    restaurant_id BIGINT,
    category_name VARCHAR(255),
    item_name VARCHAR(255),
    item_description TEXT,
    is_veg BOOLEAN,
    FOREIGN KEY (restaurant_id)REFERENCES Restaurant_information(restaurant_id)
);

""")
for restaurant in struct:
    restaurant_id = restaurant["restaurant_id"]

    for category in restaurant["menu_categories"]:
        category_name = category["category_name"]

        for item in category["items"]:
            cursor.execute("""
            INSERT INTO Restaurant_Menu (
                item_id,
                restaurant_id,
                category_name,
                item_name,
                item_description,
                is_veg
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                item["item_id"],
                restaurant_id,
                category_name,
                item["item_name"],
                item.get("item_description"),
                bool(item.get("is_veg"))
            ))

conn.commit()