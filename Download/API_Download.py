import boto3
import os
from datetime import datetime
import xarray as xr


def read_secrets():
    access = ""
    secret = ""
    with open("project.secret", "r") as f:
        f.readline()
        f.readline()
        access = f.readline().strip()

        f.readline()
        f.readline()
        secret = f.readline().strip()
    return access, secret

# I N I T

# cas
start_date = datetime(2018, 6, 1)
end_date = datetime(2023, 10, 31)

# Európa bounding box
# Longitude (X) and Latitude (Y)
# 62.915668260879706, -17.179946478321874
# 32.98421960051931, 33.798203715601865
lon_min, lon_max = -25, 45
lat_min, lat_max = 34, 72

base_prefix = "CLMS/bio-geophysical/vegetation_indices/ndvi_global_300m_10daily_v3"
config = {
    "auth_server_url": "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
    "odata_base_url": "https://catalogue.dataspace.copernicus.eu/odata/v1/Products",
    "s3_endpoint_url": "https://eodata.dataspace.copernicus.eu",
}

#S3 = /eodata/CLMS/bio-geophysical/vegetation_indices/ndvi_global_300m_10daily_v3
endpoint_url = "https://eodata.dataspace.copernicus.eu"
bucket_name = "eodata" # vieme z S3

# docasny a fin subor
tmp_dir = "/run/media/martin/KINGSTON/NDVI/tmp_global"
europe_dir = "/run/media/martin/KINGSTON/NDVI/ndvi_europe"
os.makedirs(tmp_dir, exist_ok=True)
os.makedirs(europe_dir, exist_ok=True)

# pripojenie
ACCESS_KEY, SECRET_KEY = read_secrets()
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Download and crop
current = start_date

while current <= end_date:
    year = current.strftime("%Y")
    month = current.strftime("%m")

    month_prefix = f"{base_prefix}/{year}/{month}/"
    print("Looking for",month_prefix)

    # vsetky dni v mesiaci month
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=month_prefix,
        Delimiter="/"
    )

    for common_prefix in response.get("CommonPrefixes", []):
        day_prefix = common_prefix["Prefix"]

        # najprv získame podadresáre (_nc a _cog)
        day_dirs = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=day_prefix,
            Delimiter="/"
        )

        for sub_prefix in day_dirs.get("CommonPrefixes", []):
            sub_dir = sub_prefix["Prefix"]

            print(sub_dir, end=" ")
            # zaujíma nás iba _nc/
            if sub_dir.endswith("_nc/"):
                print("✅")
                file_response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=sub_dir
                )

                for obj in file_response.get("Contents", []):
                    key = obj["Key"]

                    if key.endswith(".nc"): # malo by byt vzdy

                        print(" ↳", key)

                        filename = os.path.basename(key)
                        global_path = os.path.join(tmp_dir, filename)
                        europe_path = os.path.join(europe_dir, f"EU_{filename}")

                        if not os.path.exists(global_path):
                            print("   🔄 Downloading:", filename)
                            s3.download_file(bucket_name, key, global_path)

                        print("    ✂️ Cropping to Europe...")
                        ds = xr.open_dataset(global_path)

                        if ds.lat[0] > ds.lat[-1]:
                            ds_eu = ds.sel(
                                lon=slice(lon_min, lon_max),
                                lat=slice(lat_max, lat_min)
                            )
                        else:
                            ds_eu = ds.sel(
                                lon=slice(lon_min, lon_max),
                                lat=slice(lat_min, lat_max)
                            )

                        ds_eu.to_netcdf(europe_path)
                        ds.close()
                        ds_eu.close()

                        #os.remove(global_path)

                        print("     💾 Saved:", europe_path)
            else:
                print("⏩")

    # posun o mesiac
    if current.month == 10:
        current = current.replace(year=current.year + 1, month=6)
    else:
        current = current.replace(month=current.month + 1)

print("Download and cropping finished.")
