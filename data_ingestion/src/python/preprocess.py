import h5py
import os
import pandas as pd


def extract_from_h5(h5):
    """Extract data from h5 file split per timestamp per location
    
    Parameters:
    h5 (h5py.File): h5 object to extract data

    Returns:
    pandas.DataFrame: data extracted from h5 files
    """
    measurements = h5["X"]["value"]["X"]["value"][:]
    aux = h5["X"]["value"]["aux"]["value"][:]
    hat_x = h5["X"]["value"]["hat"]["value"]["X"]["value"][:]
    hat_t = h5["X"]["value"]["hat"]["value"]["t"]["value"][:]
    time = h5["X"]["value"]["t"]["value"][:]
    df = pd.DataFrame(measurements[:,:,0])
    df["timestamp"] = time
    values = []
    location_ids = []
    timestamps = []
    for _, row in df.iterrows():
        timestamp = row["timestamp"]
        for colname, value in row.items():
            if colname != "timestamp":
                values.append(value)
                location_ids.append(colname)
                timestamps.append(timestamp)
    return pd.DataFrame({"timestamp": timestamps, "location_id": location_ids, "measurements": values}).head(3) # for debugging delete when it goes into production


def process_file(path):
    """Load h5 file, save the extracted data and remove the original file.

    Parameters:
    path (str): path to the file

    Returns:
    None
    """
    with h5py.File(path, "r") as f:
        df = extract_from_h5(f)
        df.to_csv(path.replace("h5", "csv"), sep = ",", index = False)
    os.remove(path)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", dest = "files", type = str, nargs = "+", help = "files to be processed")
    parser.add_argument("--folder", dest = "folder", type = str, nargs = "1", help = "folder to be processed")
    args = parser.parse_args()
    if args.files is not None and len(args.files) > 0:
        for file in args.files:
            process_file(file)
    elif args.folder is not None:
        for fn in os.listdir(args.folder):
            process_file(args.folder + fn)
    else:
        for dir in ["data/2018_electric_power_data/adapt/", "data/2018_electric_power_data/train/Xm1"]:
            for fn in os.lisdir(dir):
                process_file(dir + fn)
