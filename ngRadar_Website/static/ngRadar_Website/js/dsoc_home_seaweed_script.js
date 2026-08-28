(function() {
    const imgElement = document.getElementById("new-ddm-image");
    const loadingElement = document.getElementById("image-loading");
    
    if (imgElement) {
        // Unhide the image block only after it fully fetches from SeaweedFS
        imgElement.onload = function() {
            imgElement.style.display = "block";
            loadingElement.style.display = "none";
        };
        
        // Error handling:
        imgElement.onerror = function() {
            loadingElement.innerText = "Could not retrieve image from SeaweedFS.";
        };
        
        if (imgElement.complete) {
            imgElement.onload();
        }
    }
})();