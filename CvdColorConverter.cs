using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;

namespace CVD
{
    //public class CvdColorConverter : IValueConverter
    //{
    //    public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
    //    {
    //        if (value is double cvdValue)
    //        {
    //            if (cvdValue > 0)
    //                return Brushes.Green;
    //            else if (cvdValue < 0)
    //                return Brushes.Red;
    //            else
    //                return Brushes.Gray;
    //        }
    //        return Brushes.Black;
    //    }

    //    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}

    //public class SignalDirectionColorConverter : IValueConverter
    //{
    //    public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
    //    {
    //        if (value is string direction)
    //        {
    //            switch (direction.ToLower())
    //            {
    //                case "buy":
    //                    return Brushes.Green;
    //                case "sell":
    //                    return Brushes.Red;
    //                case "watch":
    //                    return Brushes.Orange;
    //                default:
    //                    return Brushes.Gray;
    //            }
    //        }
    //        return Brushes.Black;
    //    }

    //    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
}
