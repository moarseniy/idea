
css_settings = """
    #input_row {
        display: flex;
        justify-content: center;
        align-items: center;
        width: 115%;
    }
    #input_col, #button_col {
        display: flex;
        align-items: center;
    }
    #user_input {
        width: 100%;
    }
    #submit_button {
        width: 60px;
        height: 60px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 32px;
        margin-left: 10px;
    }
    """

# Кастомный CSS для скрытия кнопок зума, скачивания и подписи
custom_css_image = """
/* Скрыть кнопку скачивания */
.gr-button[title="Download"] {
    display: none;
}

/* Скрыть кнопку зума */
.gr-button[title="Zoom"] {
    display: none;
}

/* Скрыть подпись изображения */
.gr-image-caption {
    display: none;
}

/* Установить размер изображения */
.gr-image {
    width: 300px;
    height: auto;
}
"""