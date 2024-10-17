# GUI

The GUI logic here is to try and do everything in as minimalist way as possible. So far the two javascript libraries that are in play are:

- [TomSelect](https://tom-select.js.org/), and
- [MicroModal](https://micromodal.vercel.app/).

The former is used to handle the nice "edit and dropdown" selections, and the latter for modal responses. (What is a modal response? Something that forces the user to make it go away before the rest of the page
becomes active. We need that for important responses from the backend that we really want to know the 
user has seen).

At the moment we are using both via CDNs, at some future time we should integrate them properly.