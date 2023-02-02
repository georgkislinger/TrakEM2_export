from ij import ImagePlus
from ini.trakem2 import Project
from ini.trakem2.display import Patch
from ini.trakem2.display import Display
from ij.io import FileSaver
from java.awt import Color
import sys
from ij.gui import GenericDialog

gui = GenericDialog("Export from TrakEM2")
# Create an instance of GenericDialog
gui = GenericDialog("Export from TrakEM2")

# Add some gui elements (Ok and Cancel button are present by default)
# Elements are stacked on top of each others by default (unless specified)
# gui.addMessage("")
gui.addStringField("Specify output path:", "C:/your/path")
gui.addStringField("Filename:", "section_")
gui.addCheckbox("This tickbox does nothing", True)

# We can add elements next to each other using the addToSameRow method
# gui.addToSameRow() # The next item is appended next to the tick box
gui.addChoice("Choose your desired file format (no effect)", ["tif", "jpg", "png"], "tif") # tif is default here

# We can add elements next to each other using the addToSameRow method
# gui.addToSameRow() # The next item is appended next to the tick box
gui.addChoice("Choose background color", ["black", "white"], "black") # black is default
gui.addChoice("Choose color mode", ["8bit GRAY", "8bit COLOR", "16bit GRAY", "32bit COLOR",], "8bit GRAY")


gui.addNumericField("Scaling between 1 (full resolution) and 0", 1, )
#gui.addNumericField("Number of threads NOT WORKING", 12, 0) # 0 for no decimal part

# Add a Help button in addition to the default OK/Cancel
gui.addHelp(r"github.com/georgkislinger/") # clicking the help button will open the provided URL in the default browser

# Show dialog, the rest of the code is not executed before OK or Cancel is clicked
gui.showDialog() # dont forget to actually display the dialog at some point


# If the GUI is closed by clicking OK, then recover the inputs in order of "appearance"
if gui.wasOKed():
    tarDir = gui.getNextString()
    tarName = gui.getNextString()
    inBool   = gui.getNextBoolean()
    fileFormat   = gui.getNextChoice()
    bgColor = gui.getNextChoice() # one could alternatively call the getNextChoiceIndex too
    mode = gui.getNextChoice()
    scale = gui.getNextNumber()
#    numThreads    = gui.getNextNumber() # This always return a double (ie might need to cast to int)
else:
	print("Cancelled...")
#FULL DirName
targetDir = tarDir + "/" + tarName
	
#BACKGROUND COLOR

if bgColor == "black" :
    backgroundColor = Color.black
elif bgColor == "white" :
    backgroundColor = Color.white
else:
    print ("Something wrong setting colors")
    
#COLOR MODE

if mode == "8bit GRAY" :
    colorMode = ImagePlus.GRAY8
elif mode == "16bit GRAY" :
    colorMode = ImagePlus.GRAY16
elif mode == "8bit COLOR" :
    colorMode = ImagePlus.COLOR_256
elif mode == "32bit COLOR" :
    colorMode = ImagePlus.COLOR_RGB
else:
    print ("Something wrong setting colors")

#SCALE

if mode == "8bit GRAY" :
    colorMode = ImagePlus.GRAY8
elif mode == "16bit GRAY" :
    colorMode = ImagePlus.GRAY16
elif mode == "8bit COLOR" :
    colorMode = ImagePlus.COLOR_256
elif mode == "32bit COLOR" :
    colorMode = ImagePlus.COLOR_RGB
else:
    print ("Something wrong setting colors")

# print (targetDir)
print (backgroundColor)
print (colorMode)
# print (numThreads)


#Get TrakEM2 info
#project = Project.getProjects()[0]
#layerset = project.getRootLayerSet()
#roi = layerset.get2DBounds()
#scale = 1.0

project = Project.getProjects()[0]
layerset = project.getRootLayerSet()
front = Display.getFront(project)
layerset.setMinimumDimensions()
roi = front.getRoi()
if roi is None:
	 roi=layerset.get2DBounds()
else:
	()

if fileFormat == "tif":

	for i, layer in enumerate(layerset.getLayers()):
 		print layer
 	 # Export the image here, e.g.:
  		tiles = layer.getDisplayables(Patch)
  		ip = Patch.makeFlatImage(
           ImagePlus.GRAY8,
           layer,
           roi.getBounds(),
           scale,
           tiles,
           backgroundColor,
           True)  # use the min and max of each tile

  		imp = ImagePlus("Flat montage", ip)
  		#print(type(imp))
  		FileSaver(imp).saveAsTiff(targetDir + str(i + 1).zfill(4) + ".tif")
 
elif fileFormat == "png":

	for i, layer in enumerate(layerset.getLayers()):
 		print layer
 	 # Export the image here, e.g.:
  		tiles = layer.getDisplayables(Patch)
  		ip = Patch.makeFlatImage(
           ImagePlus.GRAY8,
           layer,
           roi,
           scale,
           tiles,
           backgroundColor,
           True)  # use the min and max of each tile

  		imp = ImagePlus("Flat montage", ip)
  		#print(type(imp))
  		FileSaver(imp).saveAsTiff(targetDir + str(i + 1) + ".png")
 
elif fileFormat == "jpg":
	
	for i, layer in enumerate(layerset.getLayers()):
 		print layer
 	 # Export the image here, e.g.:
  		tiles = layer.getDisplayables(Patch)
  		ip = Patch.makeFlatImage(
           colorMode,
           layer,
           roi,
           scale,
           tiles,
           backgroundColor,
           True)  # use the min and max of each tile

  		imp = ImagePlus("Flat montage", ip)
  		#print(type(imp))
  		FileSaver(imp).saveAsTiff(targetDir + str(i + 1) + ".jpg")
else:
  print ("something wrong here...")

print("Done!")