call "C:\Users\kanzai\AppData\Local\anaconda3\Scripts\activate.bat" > "C:\Users\KAnzai\Documents\GitHub\clipper\automation\run_clipper_trips_update.txt" 2>&1
call "C:\Users\kanzai\AppData\Local\anaconda3\condabin\conda.bat" activate mtcpy-env >> "C:\Users\KAnzai\Documents\GitHub\clipper\automation\run_clipper_trips_update.txt" 2>&1
cd ..
echo %CD% >> "C:\Users\KAnzai\Documents\GitHub\clipper\automation\run_clipper_trips_update.txt" 2>&1
python "C:\Users\KAnzai\Documents\GitHub\clipper\src\update_trip_level_data.py" >> "C:\Users\KAnzai\Documents\GitHub\clipper\automation\run_clipper_trips_update.txt" 2>&1