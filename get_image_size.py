from PIL import Image
import argparse

def get_image_size(image_path):
    """Opens an image and prints its dimensions."""
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            print(f"Image: {image_path}")
            print(f"Dimensions: {width}x{height}")
    except FileNotFoundError:
        print(f"Error: Image not found at {image_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get the dimensions of an image.")
    parser.add_argument("image_path", type=str, help="The path to the image file.")
    args = parser.parse_args()
    get_image_size(args.image_path)
