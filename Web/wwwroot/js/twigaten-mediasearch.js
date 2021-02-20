'use strict';
(function () {
  const mediaDrop = document.getElementById('twigaten-media-drop');
  const mediaInput = document.getElementById('twigaten-media-input');
  const mediaForm = document.getElementById('twigaten-media-form');

  mediaDrop.addEventListener('dragover', function (event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.add('dragging');
  });
  mediaDrop.addEventListener('dragenter', function (event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.add('dragging');
  });
  mediaDrop.addEventListener('dragleave', function (event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.remove('dragging');
  });
  //ドロップしたらformを操作してPOST(雑)
  mediaDrop.addEventListener('drop', function (event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.remove('dragging');
    if (event.dataTransfer.files && 0 < event.dataTransfer.files.length) {
      const file = event.dataTransfer.files[0];
      if (file.type.startsWith('image/')) {
        event.currentTarget.classList.add('dropped');
        mediaInput.files = event.dataTransfer.files;
        mediaForm.submit();
      }
    }
  });
  //ファイルを選択したときもPOST
  mediaInput.addEventListener('change', function () {
    mediaDrop.classList.add('dropped');
    mediaForm.submit();
  });

  //戻るボタン等でこのページに来たら色を戻す
  window.addEventListener('pageshow', function () {
    mediaDrop.classList.remove('dropped');
  });
})();
