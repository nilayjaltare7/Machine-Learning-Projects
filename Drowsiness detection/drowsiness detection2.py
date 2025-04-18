import cv2
import os
from keras.models import load_model
import numpy as np
import pygame.mixer as mixer
import time

# Initialize Pygame Mixer for sound
mixer.init()
sound = mixer.Sound('alarm.wav')  # Load the alarm sound

# Load pre-trained Haar cascades for face and eyes detection
face = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_alt.xml')
leye = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_lefteye_2splits.xml')
reye = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_righteye_2splits.xml')

# Labels for eye status
lbl = ['Close', 'Open']

# Load the pre-trained CNN model for eye state classification
model = load_model('models/cnncat2.h5')

# Get the current working directory
path = os.getcwd()

# Open the default camera (0)
cap = cv2.VideoCapture(0)

# Font and variables initialization
font = cv2.FONT_HERSHEY_COMPLEX_SMALL
count = 0
score = 0
thicc = 2
rpred = [99]
lpred = [99]

# Continuous loop to capture video frames
while True:
    ret, frame = cap.read()  # Read frame from the video capture
    
    if not ret:  # Break the loop if there is no frame
        break
    
    height, width = frame.shape[:2]  # Get frame height and width

    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)  # Convert frame to grayscale

    # Detect faces in the grayscale frame
    faces = face.detectMultiScale(gray, minNeighbors=5, scaleFactor=1.1, minSize=(25, 25))
    
    # Detect left and right eyes in the grayscale frame
    left_eye = leye.detectMultiScale(gray)
    right_eye = reye.detectMultiScale(gray)

    # Draw a black rectangle at the bottom of the frame
    cv2.rectangle(frame, (0, height - 50), (200, height), (0, 0, 0), thickness=cv2.FILLED)

    # Loop through detected faces and draw rectangles around them
    for (x, y, w, h) in faces:
        cv2.rectangle(frame, (x, y), (x + w, y + h), (100, 100, 100), 1)

    # Detect and classify right eye status
    for (x, y, w, h) in right_eye:
        r_eye = frame[y:y + h, x:x + w]
        count += 1
        r_eye = cv2.cvtColor(r_eye, cv2.COLOR_BGR2GRAY)
        r_eye = cv2.resize(r_eye, (24, 24))
        r_eye = r_eye / 255
        r_eye = np.reshape(r_eye, (24, 24, -1))
        r_eye = np.expand_dims(r_eye, axis=0)
        rpred = model.predict(r_eye)
        if np.argmax(rpred) == 1:
            lbl = 'Open'
        if np.argmax(rpred) == 0:
            lbl = 'Closed'
        break

    # Detect and classify left eye status
    for (x, y, w, h) in left_eye:
        l_eye = frame[y:y + h, x:x + w]
        count += 1
        l_eye = cv2.cvtColor(l_eye, cv2.COLOR_BGR2GRAY)
        l_eye = cv2.resize(l_eye, (24, 24))
        l_eye = l_eye / 255
        l_eye = np.reshape(l_eye, (24, 24, -1))
        l_eye = np.expand_dims(l_eye, axis=0)
        lpred = model.predict(l_eye)
        if np.argmax(lpred) == 1:
            lbl = 'Open'
        if np.argmax(lpred) == 0:
            lbl = 'Closed'
        break

    # Check and update the score based on eye status
    if np.argmax(rpred) == 0 and np.argmax(lpred) == 0:
        score += 1
        cv2.putText(frame, "Closed", (10, height - 20), font, 1, (255, 255, 255), 1, cv2.LINE_AA)
    else:
        score -= 1
        cv2.putText(frame, "Open", (10, height - 20), font, 1, (255, 255, 255), 1, cv2.LINE_AA)

    # Ensure score doesn't go below 0
    if score < 0:
        score = 0
    
    # Display the score on the frame
    cv2.putText(frame, 'Score:' + str(score), (100, height - 20), font, 1, (255, 255, 255), 1, cv2.LINE_AA)
    
    # If the score reaches a threshold, trigger an alarm and draw a rectangle around the frame
    if score > 15:
        cv2.imwrite(os.path.join(path, 'image.jpg'), frame)  # Save the frame as an image
        try:
            sound.play()  # Play the alarm sound
        except:
            pass
        if thicc < 16:
            thicc += 2
        else:
            thicc -= 2
            if thicc < 2:
                thicc = 2
        cv2.rectangle(frame, (0, 0), (width, height), (0, 0, 255), thicc)

    cv2.imshow('frame', frame)  # Display the frame
    
    # Break the loop if 'q' key is pressed
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()  # Release the video capture
cv2.destroyAllWindows()  # Close all OpenCV windows
