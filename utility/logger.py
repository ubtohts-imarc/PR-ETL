import logging
import os
import datetime
import inspect

def get_logger():
    # Identify the script that initiated the logging
    frame = inspect.currentframe()
    while frame.f_back:
        frame = frame.f_back
        module = inspect.getmodule(frame)
        if module and module.__file__ and "websites" in module.__file__:
            script_name = os.path.splitext(os.path.basename(module.__file__))[0]
            break
    else:
        script_name = "unknown_script"
    
    # Create logs directory if not exists
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"{script_name}_{timestamp}.log")
    
    # Setup logger
    logger = logging.getLogger(script_name)
    if not logger.hasHandlers():  # Ensure logger is not duplicated
        logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)d - %(thread)d- %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
