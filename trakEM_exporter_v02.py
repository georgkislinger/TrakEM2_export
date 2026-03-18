from __future__ import print_function

from ij import ImagePlus, ImageStack, IJ
from ini.trakem2 import Project
from ini.trakem2.display import Patch, Display
from ij.io import FileSaver
from ij.process import ImageProcessor
from ij.gui import GenericDialog

from java.awt import Color, Rectangle
from java.io import File, FileOutputStream, BufferedOutputStream
from java.lang import Runnable, Runtime
from java.nio import ByteBuffer, ByteOrder
from java.util import ArrayList, Collections
from java.util.concurrent import Executors, TimeUnit

import json
import math
import os
import traceback


# ============================================================
# Defaults
# ============================================================

DEFAULT_VOXEL_X_NM = 4.0
DEFAULT_VOXEL_Y_NM = 4.0
DEFAULT_VOXEL_Z_NM = 100.0

DEFAULT_SCALE = 1.0
DEFAULT_VAST_TILE_SIZE = 1024
DEFAULT_OME_CHUNK_SIZE = 128
OME_ZARR_VERSION = "0.4"

# Flat-images / stack modes render a whole plane in memory.
# This is a conservative upper bound to block obviously impossible exports.
MAX_IMAGEJ_PIXELS_PER_PLANE = 2147483000

CONFIG = {}


# ============================================================
# Logging / errors
# ============================================================

def log(msg):
    print(msg)
    try:
        IJ.log(str(msg))
    except:
        pass


def fail(msg):
    IJ.error("TrakEM2 export", str(msg))
    raise RuntimeError(msg)


# ============================================================
# Utility parsing / IO
# ============================================================

def safe_float(text, default_value):
    if text is None:
        return default_value
    text = str(text).strip()
    if text == "":
        return default_value
    try:
        return float(text)
    except:
        return default_value


def safe_int(text, default_value):
    if text is None:
        return default_value
    text = str(text).strip()
    if text == "":
        return default_value
    try:
        return int(round(float(text)))
    except:
        return default_value


def ensure_dir(path):
    f = File(path)
    if not f.exists():
        if not f.mkdirs():
            fail("Could not create directory: %s" % path)


def path_exists(path):
    return File(path).exists()


def write_text_file(path, text):
    parent = File(path).getParentFile()
    if parent is not None:
        ensure_dir(parent.getAbsolutePath())
    fh = open(path, "w")
    try:
        fh.write(text)
    finally:
        fh.close()


def write_bytes(path, byte_array):
    parent = File(path).getParentFile()
    if parent is not None:
        ensure_dir(parent.getAbsolutePath())
    stream = BufferedOutputStream(FileOutputStream(path))
    try:
        stream.write(byte_array)
        stream.flush()
    finally:
        stream.close()


def json_dump_pretty(data):
    return json.dumps(data, indent=4, sort_keys=True)


# ============================================================
# Mode / metadata helpers
# ============================================================

def bytes_per_pixel_for_mode(mode_name):
    if mode_name == "8bit GRAY":
        return 1
    if mode_name == "16bit GRAY":
        return 2
    if mode_name == "32bit COLOR":
        return 3
    if mode_name == "8bit COLOR":
        return 1
    return 1


def color_mode_constant(mode_name):
    if mode_name == "8bit GRAY":
        return ImagePlus.GRAY8
    if mode_name == "16bit GRAY":
        return ImagePlus.GRAY16
    if mode_name == "8bit COLOR":
        return ImagePlus.COLOR_256
    if mode_name == "32bit COLOR":
        return ImagePlus.COLOR_RGB
    fail("Unsupported color mode: %s" % mode_name)


def background_color_from_name(name):
    if name == "black":
        return Color.black
    if name == "white":
        return Color.white
    fail("Unsupported background color: %s" % name)


def numeric_background_fill(mode_name, background_name):
    white = (background_name == "white")
    if mode_name == "8bit GRAY":
        return 255 if white else 0
    if mode_name == "16bit GRAY":
        return 65535 if white else 0
    if mode_name == "8bit COLOR":
        return 255 if white else 0
    if mode_name == "32bit COLOR":
        return None
    return 0


def zarr_dtype_for_mode(mode_name):
    if mode_name == "8bit GRAY":
        return "|u1"
    if mode_name == "16bit GRAY":
        return "<u2"
    fail("Native OME-Zarr export currently supports only 8bit GRAY and 16bit GRAY")


def omero_metadata_for_mode(dataset_name, mode_name):
    if mode_name == "8bit GRAY":
        max_value = 255
    elif mode_name == "16bit GRAY":
        max_value = 65535
    else:
        fail("Native OME-Zarr export currently supports only 8bit GRAY and 16bit GRAY")

    return {
        "id": 1,
        "name": dataset_name,
        "version": OME_ZARR_VERSION,
        "channels": [
            {
                "active": True,
                "coefficient": 1,
                "color": "FFFFFF",
                "family": "linear",
                "inverted": False,
                "label": dataset_name,
                "window": {
                    "min": 0,
                    "max": max_value,
                    "start": 0,
                    "end": max_value
                }
            }
        ],
        "rdefs": {
            "defaultZ": 0,
            "model": "greyscale"
        }
    }


# ============================================================
# Geometry / scale helpers
# ============================================================

def auto_mip_count(width, height):
    max_dim = max(int(width), int(height))
    count = 0
    w = int(width)
    h = int(height)
    while max_dim >= 512:
        w = max(1, int(math.ceil(w / 2.0)))
        h = max(1, int(math.ceil(h / 2.0)))
        count += 1
        max_dim = max(w, h)
    return count


def scaled_size_from_bounds(bounds, scale):
    return (
        max(1, int(math.ceil(bounds.width * scale))),
        max(1, int(math.ceil(bounds.height * scale)))
    )


def build_level_sizes(base_width, base_height, max_m):
    sizes = []
    w = int(base_width)
    h = int(base_height)
    for level in range(max_m + 1):
        sizes.append((w, h))
        if level < max_m:
            w = max(1, int(math.ceil(w / 2.0)))
            h = max(1, int(math.ceil(h / 2.0)))
    return sizes


def world_rect_for_output_tile(bounds, out_x, out_y, out_w, out_h, level_scale):
    if level_scale <= 0.0:
        fail("Invalid level scale: %s" % level_scale)

    x0 = bounds.x + int(math.floor(out_x / float(level_scale)))
    y0 = bounds.y + int(math.floor(out_y / float(level_scale)))
    x1 = bounds.x + int(math.ceil((out_x + out_w) / float(level_scale)))
    y1 = bounds.y + int(math.ceil((out_y + out_h) / float(level_scale)))

    bx1 = bounds.x + bounds.width
    by1 = bounds.y + bounds.height

    x0 = max(bounds.x, min(bx1, x0))
    y0 = max(bounds.y, min(by1, y0))
    x1 = max(bounds.x, min(bx1, x1))
    y1 = max(bounds.y, min(by1, y1))

    return Rectangle(
        int(x0),
        int(y0),
        max(1, int(x1 - x0)),
        max(1, int(y1 - y0))
    )


# ============================================================
# Image rendering helpers
# ============================================================

def render_flat_layer(layer, src_rect, scale, color_mode, background_color, patches=None):
    if patches is None:
        patches = layer.getDisplayables(Patch)
    return Patch.makeFlatImage(
        color_mode,
        layer,
        src_rect,
        scale,
        patches,
        background_color,
        True
    )


def processor_crop(ip, x, y, w, h):
    ip.setRoi(x, y, w, h)
    return ip.crop()


def fit_processor_to_size(ip, target_w, target_h, mode_name, background_color_name, background_color):
    crop_w = min(int(target_w), int(ip.getWidth()))
    crop_h = min(int(target_h), int(ip.getHeight()))

    ip.setRoi(0, 0, crop_w, crop_h)
    cropped = ip.crop()

    if cropped.getWidth() == target_w and cropped.getHeight() == target_h:
        return cropped

    out = cropped.createProcessor(int(target_w), int(target_h))
    if mode_name == "32bit COLOR":
        out.setColor(background_color)
        out.fill()
    else:
        out.setValue(float(numeric_background_fill(mode_name, background_color_name)))
        out.fill()
    out.insert(cropped, 0, 0)
    return out


def save_processor_as_image(ip, path, image_format):
    imp = ImagePlus("export", ip)
    saver = FileSaver(imp)
    ok = False
    if image_format == "tif":
        ok = saver.saveAsTiff(path)
    elif image_format == "png":
        ok = saver.saveAsPng(path)
    elif image_format == "jpg":
        ok = saver.saveAsJpeg(path)
    else:
        fail("Unsupported file format: %s" % image_format)
    if not ok:
        fail("Failed to save file: %s" % path)


def shorts_to_little_endian_bytes(short_array):
    buf = ByteBuffer.allocate(len(short_array) * 2)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    for i in range(len(short_array)):
        buf.putShort(short_array[i])
    return buf.array()


def processor_to_raw_bytes(ip, mode_name):
    pixels = ip.getPixels()
    if mode_name == "8bit GRAY":
        return pixels
    if mode_name == "16bit GRAY":
        return shorts_to_little_endian_bytes(pixels)
    fail("Native OME-Zarr export currently supports only 8bit GRAY and 16bit GRAY")


# ============================================================
# OME-Zarr metadata
# ============================================================

def build_omezarr_metadata(
    dataset_name,
    num_sections,
    base_width,
    base_height,
    voxel_x_nm,
    voxel_y_nm,
    voxel_z_nm,
    export_scale,
    chunk_size_xy,
    max_m,
    mode_name,
    background_name
):
    axes = [
        {"name": "c", "type": "channel"},
        {"name": "z", "type": "space", "unit": "micrometer"},
        {"name": "y", "type": "space", "unit": "micrometer"},
        {"name": "x", "type": "space", "unit": "micrometer"}
    ]

    base_x_um = (voxel_x_nm / 1000.0) / export_scale
    base_y_um = (voxel_y_nm / 1000.0) / export_scale
    base_z_um = voxel_z_nm / 1000.0

    datasets = []
    level_shapes = []
    w = int(base_width)
    h = int(base_height)

    for level in range(max_m + 1):
        datasets.append({
            "path": str(level),
            "coordinateTransformations": [
                {
                    "type": "scale",
                    "scale": [1.0, base_z_um, base_y_um * (2 ** level), base_x_um * (2 ** level)]
                }
            ]
        })
        level_shapes.append((1, num_sections, h, w))
        if level < max_m:
            w = max(1, int(math.ceil(w / 2.0)))
            h = max(1, int(math.ceil(h / 2.0)))

    group_attrs = {
        "multiscales": [
            {
                "version": OME_ZARR_VERSION,
                "name": dataset_name,
                "axes": axes,
                "datasets": datasets
            }
        ],
        "omero": omero_metadata_for_mode(dataset_name, mode_name)
    }

    zarr_arrays = []
    dtype = zarr_dtype_for_mode(mode_name)
    fill_value = numeric_background_fill(mode_name, background_name)

    for level, shape in enumerate(level_shapes):
        zarr_arrays.append({
            "level": level,
            "shape": [int(shape[0]), int(shape[1]), int(shape[2]), int(shape[3])],
            "zarray": {
                "chunks": [1, 1, chunk_size_xy, chunk_size_xy],
                "compressor": None,
                "dimension_separator": "/",
                "dtype": dtype,
                "fill_value": fill_value,
                "filters": None,
                "order": "C",
                "shape": [int(shape[0]), int(shape[1]), int(shape[2]), int(shape[3])],
                "zarr_format": 2
            }
        })

    return group_attrs, zarr_arrays, level_shapes


def write_omezarr_skeleton(
    store_path,
    dataset_name,
    num_sections,
    base_width,
    base_height,
    voxel_x_nm,
    voxel_y_nm,
    voxel_z_nm,
    export_scale,
    chunk_size_xy,
    max_m,
    mode_name,
    background_name,
    write_array_dimensions
):
    ensure_dir(store_path)
    write_text_file(os.path.join(store_path, ".zgroup"), json_dump_pretty({"zarr_format": 2}))

    group_attrs, zarr_arrays, level_shapes = build_omezarr_metadata(
        dataset_name,
        num_sections,
        base_width,
        base_height,
        voxel_x_nm,
        voxel_y_nm,
        voxel_z_nm,
        export_scale,
        chunk_size_xy,
        max_m,
        mode_name,
        background_name
    )
    write_text_file(os.path.join(store_path, ".zattrs"), json_dump_pretty(group_attrs))

    for array_meta in zarr_arrays:
        array_dir = os.path.join(store_path, str(array_meta["level"]))
        ensure_dir(array_dir)
        write_text_file(os.path.join(array_dir, ".zarray"), json_dump_pretty(array_meta["zarray"]))
        zattrs = {}
        if write_array_dimensions:
            zattrs["_ARRAY_DIMENSIONS"] = ["c", "z", "y", "x"]
        write_text_file(os.path.join(array_dir, ".zattrs"), json_dump_pretty(zattrs))

    return level_shapes


# ============================================================
# VASTlite metadata
# ============================================================

def write_vast_vsvi(
    vast_root,
    dataset_name,
    comment_text,
    first_section_number,
    last_section_number,
    num_sections,
    base_width,
    base_height,
    tile_size,
    tile_format,
    max_m,
    voxel_x_nm,
    voxel_y_nm,
    voxel_z_nm,
    export_scale,
    mode_name
):
    rows = int(math.ceil(base_height / float(tile_size)))
    cols = int(math.ceil(base_width / float(tile_size)))

    bytes_per_pixel = bytes_per_pixel_for_mode(mode_name)
    if bytes_per_pixel not in (1, 3):
        fail("VASTlite export from this script is intended for 8-bit grayscale or RGB imagery. 16-bit grayscale is not a good VAST target.")

    vsvi = {
        "Comment": comment_text,
        "ServerType": "imagetiles",
        "SourceFileNameTemplate": ".\\mip0\\slice%04d\\%04d_tr%d-tc%d." + tile_format,
        "SourceParamSequence": "ssrc",
        "SourceMinS": int(first_section_number),
        "SourceMaxS": int(last_section_number),
        "SourceMinR": 1,
        "SourceMaxR": int(rows),
        "SourceMinC": 1,
        "SourceMaxC": int(cols),
        "MipMapFileNameTemplate": ".\\mip%d\\slice%04d\\%04d_tr%d-tc%d." + tile_format,
        "MipMapParamSequence": "mssrc",
        "SourceMinM": 1,
        "SourceMaxM": int(max_m),
        "SourceTileSizeX": int(tile_size),
        "SourceTileSizeY": int(tile_size),
        "SourceBytesPerPixel": int(bytes_per_pixel),
        "MissingImagePolicy": "nearest",
        "TargetDataSizeX": int(base_width),
        "TargetDataSizeY": int(base_height),
        "TargetDataSizeZ": int(num_sections),
        "OffsetX": 0,
        "OffsetY": 0,
        "OffsetZ": 0,
        "OffsetMip": 0,
        "TargetVoxelSizeXnm": float(voxel_x_nm) / float(export_scale),
        "TargetVoxelSizeYnm": float(voxel_y_nm) / float(export_scale),
        "TargetVoxelSizeZnm": float(voxel_z_nm),
        "TargetLayerName": comment_text
    }

    write_text_file(os.path.join(vast_root, dataset_name + ".vsvi"), json_dump_pretty(vsvi))


# ============================================================
# Worker task
# ============================================================

class SectionExportTask(Runnable):

    def __init__(self, z_rel_index, section_number, layer):
        self.z_rel_index = int(z_rel_index)
        self.section_number = int(section_number)
        self.layer = layer

    def run(self):
        try:
            patches = self.layer.getDisplayables(Patch)

            # Flat images still render the full plane in memory.
            if CONFIG["write_flat_images"]:
                ip = render_flat_layer(
                    self.layer,
                    CONFIG["bounds"],
                    CONFIG["scale"],
                    CONFIG["color_mode"],
                    CONFIG["background_color"],
                    patches
                )
                flat_name = CONFIG["base_name"] + str(self.section_number).zfill(4) + "." + CONFIG["flat_format"]
                flat_path = os.path.join(CONFIG["flat_output_dir"], flat_name)
                save_processor_as_image(ip, flat_path, CONFIG["flat_format"])

            # Tile / chunk-based rendering for large data:
            # never build the full plane for VAST or OME-Zarr.
            if CONFIG["write_vast"] or CONFIG["write_omezarr"]:
                for level, (level_w, level_h) in enumerate(CONFIG["level_sizes"]):
                    level_scale = CONFIG["scale"] / float(2 ** level)

                    # -------------------------
                    # VAST: fixed-size padded tiles
                    # -------------------------
                    if CONFIG["write_vast"]:
                        if level == 0:
                            level_dir = os.path.join(
                                CONFIG["vast_root"],
                                "mip0",
                                "slice%04d" % self.section_number
                            )
                        else:
                            level_dir = os.path.join(
                                CONFIG["vast_root"],
                                "mip%d" % level,
                                "slice%04d" % self.section_number
                            )
                        ensure_dir(level_dir)

                        rows_vast = int(math.ceil(level_h / float(CONFIG["vast_tile_size"])))
                        cols_vast = int(math.ceil(level_w / float(CONFIG["vast_tile_size"])))

                        for row in range(rows_vast):
                            out_y = row * CONFIG["vast_tile_size"]
                            for col in range(cols_vast):
                                out_x = col * CONFIG["vast_tile_size"]

                                world_rect = world_rect_for_output_tile(
                                    CONFIG["bounds"],
                                    out_x,
                                    out_y,
                                    CONFIG["vast_tile_size"],
                                    CONFIG["vast_tile_size"],
                                    level_scale
                                )

                                tile_ip = render_flat_layer(
                                    self.layer,
                                    world_rect,
                                    level_scale,
                                    CONFIG["color_mode"],
                                    CONFIG["background_color"],
                                    patches
                                )

                                tile_ip = fit_processor_to_size(
                                    tile_ip,
                                    CONFIG["vast_tile_size"],
                                    CONFIG["vast_tile_size"],
                                    CONFIG["mode_name"],
                                    CONFIG["background_name"],
                                    CONFIG["background_color"]
                                )

                                tile_name = "%04d_tr%d-tc%d.%s" % (
                                    self.section_number,
                                    row + 1,
                                    col + 1,
                                    CONFIG["vast_tile_format"]
                                )
                                tile_path = os.path.join(level_dir, tile_name)
                                save_processor_as_image(tile_ip, tile_path, CONFIG["vast_tile_format"])

                    # -------------------------
                    # OME-Zarr: exact chunk sizes on borders
                    # -------------------------
                    if CONFIG["write_omezarr"]:
                        rows_ome = int(math.ceil(level_h / float(CONFIG["ome_chunk_size_xy"])))
                        cols_ome = int(math.ceil(level_w / float(CONFIG["ome_chunk_size_xy"])))

                        for row in range(rows_ome):
                            out_y = row * CONFIG["ome_chunk_size_xy"]
                            desired_h = min(CONFIG["ome_chunk_size_xy"], level_h - out_y)

                            for col in range(cols_ome):
                                out_x = col * CONFIG["ome_chunk_size_xy"]
                                desired_w = min(CONFIG["ome_chunk_size_xy"], level_w - out_x)

                                world_rect = world_rect_for_output_tile(
                                    CONFIG["bounds"],
                                    out_x,
                                    out_y,
                                    desired_w,
                                    desired_h,
                                    level_scale
                                )

                                chunk_ip = render_flat_layer(
                                    self.layer,
                                    world_rect,
                                    level_scale,
                                    CONFIG["color_mode"],
                                    CONFIG["background_color"],
                                    patches
                                )

                                chunk_ip = fit_processor_to_size(
                                    chunk_ip,
                                    desired_w,
                                    desired_h,
                                    CONFIG["mode_name"],
                                    CONFIG["background_name"],
                                    CONFIG["background_color"]
                                )

                                chunk_path = os.path.join(
                                    CONFIG["omezarr_root"],
                                    str(level),
                                    "0",
                                    str(self.z_rel_index),
                                    str(row),
                                    str(col)
                                )
                                raw = processor_to_raw_bytes(chunk_ip, CONFIG["mode_name"])
                                write_bytes(chunk_path, raw)

            CONFIG["completed"].add(self.section_number)
            log("Finished section %04d" % self.section_number)

        except:
            CONFIG["errors"].add(traceback.format_exc())


# ============================================================
# GUI
# ============================================================

def build_dialog(layerset_size):
    gui = GenericDialog("Export from TrakEM2")
    gui.addDirectoryField("Output directory", "C:/your/path", 50)
    gui.addStringField("Dataset / filename prefix", "section_", 30)
    gui.addStringField("Comment / layer name", "TrakEM2 export", 40)

    gui.addChoice(
        "Export mode",
        [
            "Flat images",
            "Show stack",
            "OME-Zarr (0.4)",
            "VASTlite tiles + .vsvi",
            "OME-Zarr + VASTlite"
        ],
        "OME-Zarr (0.4)"
    )

    gui.addChoice("Flat file format", ["tif", "png", "jpg"], "tif")
    gui.addChoice("VAST tile format", ["png", "jpg"], "png")
    gui.addChoice("Background color", ["black", "white"], "black")
    gui.addChoice("Color mode", ["8bit GRAY", "16bit GRAY", "8bit COLOR", "32bit COLOR"], "8bit GRAY")
    gui.addStringField("Scale (0 < s <= 1, blank = 1)", "1", 8)

    gui.addCheckbox("Export full stack?", True)
    gui.addSlider("First section to export", 1, layerset_size, 1)
    gui.addSlider("Last section to export", 1, layerset_size, layerset_size)

    gui.addMessage("Calibration")
    gui.addStringField("Voxel size X (nm, blank = 4)", str(DEFAULT_VOXEL_X_NM), 8)
    gui.addStringField("Voxel size Y (nm, blank = 4)", str(DEFAULT_VOXEL_Y_NM), 8)
    gui.addStringField("Voxel size Z (nm, blank = 100)", str(DEFAULT_VOXEL_Z_NM), 8)

    gui.addMessage("Multiscale / tiling")
    gui.addStringField("OME chunk size XY (blank = 128)", str(DEFAULT_OME_CHUNK_SIZE), 8)
    gui.addStringField("VAST tile size XY (blank = 1024)", str(DEFAULT_VAST_TILE_SIZE), 8)
    gui.addStringField("Pyramid levels (blank = auto)", "", 8)

    gui.addMessage("Parallelism and compatibility")
    gui.addStringField("Max workers (blank = all available)", "", 8)
    gui.addCheckbox("Write _ARRAY_DIMENSIONS metadata", True)

    gui.addHelp("https://github.com/georgkislinger/")
    return gui


# ============================================================
# Main
# ============================================================

def main():
    projects = Project.getProjects()
    if projects is None or len(projects) == 0:
        fail("No open TrakEM2 project found")

    project = projects[0]
    layerset = project.getRootLayerSet()
    front = Display.getFront(project)
    layerset.setMinimumDimensions()

    roi_obj = None
    if front is not None:
        roi_obj = front.getRoi()
    if roi_obj is None:
        bounds = layerset.get2DBounds()
    else:
        bounds = roi_obj.getBounds()

    gui = build_dialog(layerset.size())
    gui.showDialog()
    if not gui.wasOKed():
        log("Cancelled...")
        return

    output_dir = gui.getNextString().strip()
    base_name = gui.getNextString().strip()
    comment_text = gui.getNextString().strip()
    export_mode = gui.getNextChoice()
    flat_format = gui.getNextChoice()
    vast_tile_format = gui.getNextChoice()
    background_name = gui.getNextChoice()
    mode_name = gui.getNextChoice()
    scale = safe_float(gui.getNextString(), DEFAULT_SCALE)
    full_stack = gui.getNextBoolean()
    min_sec = int(round(gui.getNextNumber()))
    max_sec = int(round(gui.getNextNumber()))
    voxel_x_nm = safe_float(gui.getNextString(), DEFAULT_VOXEL_X_NM)
    voxel_y_nm = safe_float(gui.getNextString(), DEFAULT_VOXEL_Y_NM)
    voxel_z_nm = safe_float(gui.getNextString(), DEFAULT_VOXEL_Z_NM)
    ome_chunk_size_xy = safe_int(gui.getNextString(), DEFAULT_OME_CHUNK_SIZE)
    vast_tile_size = safe_int(gui.getNextString(), DEFAULT_VAST_TILE_SIZE)
    pyramid_text = gui.getNextString().strip()
    workers_text = gui.getNextString().strip()
    write_array_dimensions = gui.getNextBoolean()

    if output_dir == "":
        fail("Please provide an output directory")
    if base_name == "":
        fail("Please provide a dataset or filename prefix")
    if comment_text == "":
        comment_text = base_name

    if scale <= 0.0 or scale > 1.0:
        fail("Scale must satisfy 0 < scale <= 1")
    if ome_chunk_size_xy <= 0:
        fail("OME chunk size must be a positive integer")
    if vast_tile_size <= 0:
        fail("VAST tile size must be a positive integer")

    total_layers = layerset.size()
    if full_stack:
        min_index = 0
        max_index = total_layers - 1
    else:
        min_index = max(0, min(total_layers - 1, min_sec - 1))
        max_index = max(0, min(total_layers - 1, max_sec - 1))
        if max_index < min_index:
            fail("Last section must be >= first section")

    layers = list(layerset.getLayers(min_index, max_index))
    if len(layers) == 0:
        fail("No layers selected for export")

    available_workers = Runtime.getRuntime().availableProcessors()
    max_workers = safe_int(workers_text, available_workers)
    if max_workers < 1:
        max_workers = 1

    write_flat_images = (export_mode == "Flat images")
    show_stack = (export_mode == "Show stack")
    write_omezarr = (export_mode in ("OME-Zarr (0.4)", "OME-Zarr + VASTlite"))
    write_vast = (export_mode in ("VASTlite tiles + .vsvi", "OME-Zarr + VASTlite"))

    if write_omezarr and mode_name not in ("8bit GRAY", "16bit GRAY"):
        fail("Native OME-Zarr export currently supports only 8bit GRAY and 16bit GRAY")

    if write_vast and mode_name == "16bit GRAY":
        fail("VASTlite export is best targeted to 8-bit grayscale or RGB; 16-bit grayscale is not handled here")
    if write_vast and mode_name == "8bit COLOR":
        fail("For VASTlite, prefer 32bit COLOR instead of indexed 8bit COLOR so SourceBytesPerPixel is unambiguous")
    if write_vast and vast_tile_size % 16 != 0:
        fail("For VASTlite, tile size should be a multiple of 16")

    color_mode = color_mode_constant(mode_name)
    background_color = background_color_from_name(background_name)

    # Determine output size without rendering the whole section
    base_width, base_height = scaled_size_from_bounds(bounds, scale)

    # Flat images / stack still need a full in-memory plane
    if (write_flat_images or show_stack) and (base_width * base_height > MAX_IMAGEJ_PIXELS_PER_PLANE):
        fail(
            "Flat images / Show stack render a full 2D plane in memory and cannot handle "
            "an output of %d x %d pixels. Use OME-Zarr or VASTlite for datasets this large."
            % (base_width, base_height)
        )

    user_mips = safe_int(pyramid_text, None)
    if user_mips is None:
        max_m = auto_mip_count(base_width, base_height)
    else:
        max_m = max(0, int(user_mips))

    if write_vast and max_m < 1:
        max_m = 1

    level_sizes = build_level_sizes(base_width, base_height, max_m)

    ensure_dir(output_dir)

    flat_output_dir = output_dir
    omezarr_root = os.path.join(output_dir, base_name + ".zarr")
    vast_root = os.path.join(output_dir, base_name + "_vastlite")

    if write_omezarr and path_exists(omezarr_root):
        fail(
            "OME-Zarr output folder already exists: %s\n"
            "Please choose a different prefix or remove the existing folder." % omezarr_root
        )
    if write_vast and path_exists(vast_root):
        fail(
            "VASTlite output folder already exists: %s\n"
            "Please choose a different prefix or remove the existing folder." % vast_root
        )

    num_sections = len(layers)
    first_section_number = min_index + 1
    last_section_number = max_index + 1

    if write_omezarr:
        write_omezarr_skeleton(
            omezarr_root,
            comment_text,
            num_sections,
            base_width,
            base_height,
            voxel_x_nm,
            voxel_y_nm,
            voxel_z_nm,
            scale,
            ome_chunk_size_xy,
            max_m,
            mode_name,
            background_name,
            write_array_dimensions
        )

    if write_vast:
        ensure_dir(vast_root)
        write_vast_vsvi(
            vast_root,
            base_name,
            comment_text,
            first_section_number,
            last_section_number,
            num_sections,
            base_width,
            base_height,
            vast_tile_size,
            vast_tile_format,
            max_m,
            voxel_x_nm,
            voxel_y_nm,
            voxel_z_nm,
            scale,
            mode_name
        )

    CONFIG.clear()
    CONFIG.update({
        "bounds": bounds,
        "scale": scale,
        "background_name": background_name,
        "background_color": background_color,
        "mode_name": mode_name,
        "color_mode": color_mode,
        "base_name": base_name,
        "comment_text": comment_text,
        "flat_format": flat_format,
        "vast_tile_format": vast_tile_format,
        "ome_chunk_size_xy": ome_chunk_size_xy,
        "vast_tile_size": vast_tile_size,
        "max_m": max_m,
        "level_sizes": level_sizes,
        "write_flat_images": write_flat_images,
        "show_stack": show_stack,
        "write_omezarr": write_omezarr,
        "write_vast": write_vast,
        "flat_output_dir": flat_output_dir,
        "omezarr_root": omezarr_root,
        "vast_root": vast_root,
        "errors": Collections.synchronizedList(ArrayList()),
        "completed": Collections.synchronizedList(ArrayList())
    })

    if show_stack:
        stack = ImageStack(base_width, base_height)
        for rel_i, layer in enumerate(layers):
            section_number = min_index + rel_i + 1
            ip = render_flat_layer(layer, bounds, scale, color_mode, background_color)
            stack.addSlice(str(section_number).zfill(4), ip)
        out = ImagePlus("Stack", stack)
        out.show()
        log("Done!")
        return

    pool = Executors.newFixedThreadPool(max_workers)
    try:
        for rel_i, layer in enumerate(layers):
            section_number = min_index + rel_i + 1
            pool.submit(SectionExportTask(rel_i, section_number, layer))
    finally:
        pool.shutdown()
        pool.awaitTermination(365, TimeUnit.DAYS)

    if CONFIG["errors"].size() > 0:
        raise RuntimeError("Export failed:\n" + "\n---\n".join([str(x) for x in list(CONFIG["errors"])]))

    log("Done!")


if __name__ == "__main__":
    main()