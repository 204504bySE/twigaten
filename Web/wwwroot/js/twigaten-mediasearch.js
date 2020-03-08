'use strict';
(function () {
    const mediaDrop = document.getElementById('twigaten-media-drop');
    const mediaInput = document.getElementById('twigaten-media-input');
    const mediaForm = document.getElementById('twigaten-media-form');

    const defaultDropStyle = mediaDrop.style.cssText;
    const draggingStyle = 'background-color:#7fffd4;' + defaultDropStyle;
    const droppedStyle = 'background-color:#696969;' + defaultDropStyle;

    mediaDrop.addEventListener('dragover', (event) => {
        event.preventDefault();
        event.stopPropagation();
        event.currentTarget.style.cssText = draggingStyle;
    });
    mediaDrop.addEventListener('dragenter', (event) => {
        event.preventDefault();
        event.stopPropagation();
        event.currentTarget.style.cssText = draggingStyle;
    });
    mediaDrop.addEventListener('dragleave', (event) => {
        event.preventDefault();
        event.stopPropagation();
        event.currentTarget.style.cssText = defaultDropStyle;
    });
    //ドロップしたらformを操作してPOST(雑)
    mediaDrop.addEventListener('drop', (event) => {
        event.preventDefault();
        event.stopPropagation();
        event.currentTarget.style.cssText = defaultDropStyle;
        if (event.dataTransfer.files && 0 < event.dataTransfer.files.length) {
            const file = event.dataTransfer.files[0];
            if (file.type.startsWith('image/')) {
                event.currentTarget.style.cssText = droppedStyle;
                mediaInput.files = event.dataTransfer.files;
                mediaForm.submit();
            }
        }
    });
    //ファイルを選択したときもPOST
    mediaInput.addEventListener('change', () => {
        event.currentTarget.style.cssText = droppedStyle;
        mediaForm.submit();
    });
})();